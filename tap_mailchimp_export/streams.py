import singer
from .schemas import IDS
import requests
import pendulum
import uuid
import json
from collections import defaultdict

import backoff

logger = singer.get_logger()


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
    CAMPAIGN_SUBSCRIBER_ACTIVITY = [IDS.CAMPAIGN_SUBSRIBER_ACTIVITY, "timestamp"]
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


def get_latest_record_timestamp(records, last_updated):
    if records:
        return max(max([r['timestamp'] for r in records]),
                   last_updated)
    else:
        return last_updated


def get_latest_list_update(record, last_updated):
    if record:
        return max(record['LAST_CHANGED'], last_updated)
    else:
        return last_updated


def convert_to_iso_string(date):
    return pendulum.parse(date).to_iso8601_string()


@backoff.on_exception(backoff.expo,
                      requests.exceptions.ConnectionError,
                      max_tries=10,
                      factor=2)
def run_campaign_request(ctx, c, stream, last_updated):

    with ctx.client.put(
            'campaignSubscriberActivity', c, last_updated
    ) as res:
        for r in res.iter_lines():
            if r:
                records = transform_event(r, c)
                write_records(stream, records)
                last_updated[c['id']] = convert_to_iso_string(
                    get_latest_record_timestamp(
                        records,
                        last_updated[c['id']]
                    ))


@backoff.on_exception(backoff.expo,
                      requests.exceptions.ConnectionError,
                      max_tries=10,
                      factor=2)
def run_list_request(ctx, l, stream, last_updated):
    header = None

    with ctx.client.put(
            'list', l, last_updated
    ) as res:
        for r in res.iter_lines():
            if r:
                if header:
                    record = dict(
                        zip(header, json.loads(r.decode('utf-8'))))
                    write_records(stream, [record])
                    last_updated[l['id']] = convert_to_iso_string(
                        get_latest_list_update(
                            record,
                            last_updated[l['id']]
                        ))
                else:
                    header = json.loads(r.decode('utf-8'))


def call_stream_incremental(ctx, stream):
    last_updated = ctx.get_bookmark(BOOK.return_bookmark_path(stream)) or \
                   defaultdict(str)

    stream_resource = stream.split('_')[0]
    for e in getattr(ctx, stream_resource + 's'):
        logger.info('querying campaign: %s' % e['id'])

        ctx.update_latest(e['id'], last_updated)

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
    records = ctx.mailsnake.__getattr__(stream)()['data']
    write_records(stream, records)

    getattr(ctx, 'save_%s_meta' % stream)(records)


def save_state(ctx, stream, bk):
    ctx.set_bookmark(BOOK.return_bookmark_path(stream), bk)
    ctx.write_state()
