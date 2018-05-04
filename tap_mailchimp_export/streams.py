import singer
from .schemas import IDS
import time
import pendulum
import uuid
import json
from collections import defaultdict
from time import sleep

import backoff

logger = singer.get_logger()

BATCH_SIZE = 500
PAGE_SIZE = 500

class RemoteDisconnected(Exception):
    pass


def metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))


def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)


class BOOK(object):
    CAMPAIGNS = [IDS.CAMPAIGNS]
    CAMPAIGN_SUBSCRIBER_ACTIVITY = [IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY, "timestamp"]
    LISTS = [IDS.LISTS]
    LIST_MEMBERS = [IDS.LIST_MEMBERS, "LAST_CHANGED"]

    @classmethod
    def return_bookmark_path(cls, stream):
        return getattr(cls, stream.upper())

    @classmethod
    def get_incremental_syncs(cls):
        syncs = []
        for k, v in cls.__dict__.items():
            if not k.startswith("__") and not isinstance(v, classmethod):
                if len(v) > 1:
                    syncs.append(k)

        return syncs

    @classmethod
    def get_full_syncs(cls):
        syncs = []
        for k, v in cls.__dict__.items():
            if not k.startswith("__") and not isinstance(v, classmethod):
                if len(v) == 1:
                    syncs.append(k)

        return syncs


def sync(ctx):
    # do full syncs first as they are used later
    for stream in ctx.selected_stream_ids:
        if stream.upper() in BOOK.get_full_syncs():
            call_stream_full(ctx, stream)

    for stream in ctx.selected_stream_ids:
        if stream.upper() in BOOK.get_incremental_syncs():
            bk = call_stream_incremental(ctx, stream)
            save_state(ctx, stream, bk)


def transform_event(record, campaign):
    """
    {'test@example.com':
    [{'action': 'open', 'timestamp': '2018-04-01 22:23:21', 'url': None, 'ip': '66.249.88.148'},
    {'action': 'click', 'timestamp': '2018-04-01 22:46:10', 'url': 'http://www.mailchimp.com/kb/article/im-using-the-style-designer-and-i-cant-get-my-formatting-to-change', 'ip': '98.113.183.151'}]}
    :param record: contact level record resource from MC
    :param campaign: Campaign metadata dict
    :yield: enriched events to support downstream analytics
    """

    record = record.decode('utf-8')

    try:
        (email, events), = json.loads(record).items()
    except ValueError:
        events = []

    new_events = []

    for event in events:
        new_events.append({
            'email': email,
            'campaign_id': campaign['id'],
            'campaign_title': campaign['title'],
            'list_id': campaign['list_id'],
            'action': event['action'],
            'timestamp': event['timestamp'],
            'url': event['url'],
            'uuid': str(uuid.uuid1()),
        })

    return new_events


def get_latest_record_timestamp(records, last_updated, time_key):
    if records:
        return max(max([r[time_key] for r in records]),
                   last_updated)
    else:
        return last_updated


def write_records_and_update_state(entity, stream,
                                   batched_records, last_updated):
    write_records(stream, batched_records)
    last_updated[entity['id']] = convert_to_iso_string(
        get_latest_record_timestamp(
            batched_records,
            last_updated[entity['id']],
            BOOK.return_bookmark_path(stream)[1]
        ))


def convert_to_iso_string(date):
    return pendulum.parse(date).to_iso8601_string()


def run_campaign_request(ctx, c, stream, last_updated, retries=0):
    batched_records = []

    if retries < 3:
        try:
            with ctx.client.put(
                    'campaignSubscriberActivity', c, last_updated
            ) as res:
                for r in res.iter_lines():
                    sleep(0.1)
                    if r:
                        batched_records = batched_records + transform_event(r, c)

                        if len(batched_records) > 500:
                            write_records_and_update_state(
                                c, stream, batched_records, last_updated)

                            batched_records = []

                if batched_records:
                    write_records_and_update_state(
                        c, stream, batched_records, last_updated)
        except Exception as e:
            logger.info(e)
            logger.info('Waiting 30 seconds - then retrying')
            time.sleep(30)
            retries += 1
            run_campaign_request(ctx, c, stream, last_updated, retries)

    else:
        logger.info('Too many fails for %s, continuing to others' % c['id'])


def run_list_request(ctx, l, stream, last_updated, retries=0):
    header = None
    batched_records = []

    if retries < 3:
        try:
            with ctx.client.put(
                    'list', l, last_updated
            ) as res:
                for r in res.iter_lines():
                    sleep(0.1)
                    if r:
                        if header:
                            batched_records = batched_records + [dict(
                                zip(header, json.loads(r.decode('utf-8'))))]

                            if len(batched_records) > 500:
                                write_records_and_update_state(
                                    l, stream, batched_records, last_updated)

                                batched_records = []
                        else:
                            header = json.loads(r.decode('utf-8'))

                if batched_records:
                    write_records_and_update_state(
                        l, stream, batched_records, last_updated)

        except Exception as e:
            logger.info(e)
            logger.info('Waiting 30 seconds - then retrying')
            time.sleep(30)
            retries += 1
            run_list_request(ctx, l, stream, last_updated, retries)

    else:
        logger.info('Too many fails for %s, continuing to others' % l['id'])

def pare_records(new_records, last_updated):
    pared_records = []
    for record in new_records:
        activity = record['activity']
        if not activity:
            continue
        record['activity'] = [
            action for action in activity if activity['timestamp'] >
            last_updated 
        ]
        if record['activity']:
            pared_records.append(record)
    return pared_records

def run_incremental_request_v3(ctx, entity, stream, last_updated, retries=0):
    offset = 0
    batched_records = []
    total_items = 0
    items_recieved = 0

    while True:
        params = {
            'offset': offset,
            'count': PAGE_SIZE
        }
        if stream == IDS.LIST_MEMBERS:
            params['since_last_changed'] = last_updated

        try:
            response = ctx.client.GET_v3(stream, params, item_id=entity['id'])
        except Exception as e:
            if retries >= 3:
                raise e
            retries += 1
            logger.info(e)
            logger.info('Waiting 30 seconds - then retrying')
            time.sleep(30)
            run_incremental_request_v3(ctx, entity, stream, last_updated, retries)

        content = json.loads(response.content)
        if stream == IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY:
            records = content['emails']
            batched_records += pare_records(records, last_updated)
        else:
            records = content['members']
            batched_records += records
        total_items = content['total_items']
        items_recieved += len(records)
        logger.info('Received {}/{} records'.format(items_recieved,total_items))
        offset += len(records)

        if len(batched_records) > BATCH_SIZE:
            write_records_and_update_state(
                entity, stream, batched_records, last_updated
            )
            batched_records = []

        if items_recieved >= total_items or len(records) == 0:
            break

def call_stream_incremental(ctx, stream):
    last_updated = ctx.get_bookmark(BOOK.return_bookmark_path(stream)) or \
                   defaultdict(str)

    stream_resource = stream.split('_')[0]
    for e in getattr(ctx, stream_resource + 's'):
        ctx.update_latest(e['id'], last_updated)

        logger.info('querying {stream} id: {id}, since: {since}'.format(
            stream=stream_resource,
            id=e['id'],
            since=last_updated[e['id']],
        ))

        handlers = {
            'campaign': run_campaign_request,
            'list': run_list_request
        }

        handlers[stream_resource](ctx, e, stream, last_updated)

        ctx.set_bookmark_and_write_state(
            BOOK.return_bookmark_path(stream),
            last_updated)

    return last_updated


def call_stream_full(ctx, stream):
    records = []
    offset = 0
    while True:
        response = ctx.client.GET_v3(stream, params={'offset': offset})
        content = json.loads(response.content)
        records += content[stream]
        item_count = content['total_items']
        if len(content[stream]) == 0:
            break
        offset += len(content[stream])
    write_records(stream, records)

    getattr(ctx, 'save_%s_meta' % stream)(records)


def save_state(ctx, stream, bk):
    ctx.set_bookmark(BOOK.return_bookmark_path(stream), bk)
    ctx.write_state()
