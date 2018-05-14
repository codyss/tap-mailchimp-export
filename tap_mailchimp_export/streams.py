import singer
from .schemas import IDS
from .context import convert_to_mc_date
import time
import pendulum
import uuid
import json
import pytz
from collections import defaultdict
from time import sleep
from dateutil import parser
from dateutil.tz import tzutc
from datetime import datetime, timedelta

import backoff

logger = singer.get_logger()

BATCH_SIZE = 1000
PAGE_SIZE = 1000
LOOKBACK_DAYS = 7

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

    # record = record.decode('utf-8')
    obj = json.loads(record)
    if 'error' in obj.keys():
        raise Exception(record)

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

def run_export_request(ctx, e, stream, last_updated, retries=0):
    header = None
    batched_records = []

    if retries < 3:
        try:
            with ctx.client.post(
                    stream, e, last_updated
            ) as res:
                for r in res.iter_lines():
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
            run_export_request(ctx, l, stream, last_updated, retries)

    else:
        logger.info('Too many fails for %s, continuing to others' % l['id'])

def run_v3_request(ctx, e, stream, last_updated, retries=0):
    pass

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
            'campaign': run_export_request,
            'list': run_export_request
        }
        handlers[stream_resource](ctx, e, stream, last_updated)

        ctx.set_bookmark_and_write_state(
            BOOK.return_bookmark_path(stream),
            last_updated)

    return last_updated

def get_since_date():
    date_obj = datetime.now()
    new_date = date_obj - timedelta(days=LOOKBACK_DAYS)
    return new_date.strftime("%Y-%m-%d")

def call_stream_full(ctx, stream):
    records = []
    offset = 0
    since_date = ctx.get_start_date() or self.get_since_date()
    while True:
        params = {'offset': offset}
        if stream == IDS.CAMPAIGNS:
            params['since_send_time'] = since_date
            params['status'] = 'sent'
        elif stream == IDS.LISTS:
            params['since_send_time'] = since_date

        response = ctx.client.GET_v3(stream, params=params)
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
