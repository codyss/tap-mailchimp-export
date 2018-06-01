import singer
from .schemas import (
    IDS, V3_API_INDEX_NAMES, V3_SINCE_KEY, INTERMEDIATE_STREAMS, SUB_STREAMS
)
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
    """
    The streams defined here outline the items that for which we're going to
    gather data. Those with two items are incremental streams, and the second
    item is the time key used to save the state for items in that stream.

    NB: There are a few other endpoints hit that are used to connect full
    streams to incremental streams
    """
    CAMPAIGNS = [IDS.CAMPAIGNS]
    CAMPAIGN_SUBSCRIBER_ACTIVITY = [IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY, "timestamp"]
    LISTS = [IDS.LISTS]
    LIST_MEMBERS = [IDS.LIST_MEMBERS, "last_changed"]
    CAMPAIGN_UNSUBSCRIBES = [IDS.CAMPAIGN_UNSUBSCRIBES, "timestamp"]
    AUTOMATION_WORKFLOWS = [IDS.AUTOMATION_WORKFLOWS]
    AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY = [IDS.AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY, "timestamp"]
    AUTOMATION_WORKFLOW_UNSUBSCRIBES = [IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES, "timestamp"]


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
    """
    This is the main method of the tap. We perform the full streams first, as
    they are used in the incremental streams.

    We then perform a set of
    intermediate streams. These are streams whose data we don't save, whose ids
    are necessary to query incrementals downstream.

    Lastly, we perform our incremental syncs
    """
    for stream in ctx.selected_stream_ids:
        if stream.upper() in BOOK.get_full_syncs():
            call_stream_full(ctx, stream)

    for stream in ctx.selected_stream_ids:
        if stream in INTERMEDIATE_STREAMS:
            for entity in getattr(ctx, stream):
                call_stream_full(
                    ctx, INTERMEDIATE_STREAMS[stream], entity['id']
                )

    for stream in ctx.selected_stream_ids:
        if stream.upper() in BOOK.get_incremental_syncs():
            bk = call_stream_incremental(ctx, stream)
            save_state(ctx, stream, bk)

def transform_send_time(sent_at):
    """
    Convert a sent at campaign time to the same timestamp format as the
    timestamps returned by clicks/opens.
    2018-04-12T13:30:00+00:00  -->  2018-04-12 20:52:45
    """
    if 'T' not in sent_at:
        return sent_at
    return sent_at.replace('T', ' ')[:-6]

def transform_event(record, campaign, include_sends):
    """
    {'test@example.com':
    [{'action': 'open', 'timestamp': '2018-04-01 22:23:21', 'url': None, 'ip': '66.249.88.148'},
    {'action': 'click', 'timestamp': '2018-04-01 22:46:10', 'url': 'http://www.mailchimp.com/kb/article/im-using-the-style-designer-and-i-cant-get-my-formatting-to-change', 'ip': '98.113.183.151'}]}
    :param record: contact level record resource from MC
    :param campaign: Campaign metadata dict
    :yield: enriched events to support downstream analytics
    """
    record = record.decode('utf-8')
    obj = json.loads(record)
    if 'error' in obj.keys():
        raise Exception(record)

    try:
        (email, events), = json.loads(record).items()
    except ValueError:
        events = []

    new_events = []


    if 'workflow_id' not in campaign and include_sends:
        # We're backfilling, so we'll want to add a send event here
        new_events.append({
            'email': email,
            'action': 'send',
            'timestamp': transform_send_time(campaign['sent_at']),
            'campaign_id': campaign['id'],
            'campaign_title': campaign['title'],
            'list_id': campaign['list_id']
        })

    for event in events:
        # Get relevant information on each action (send, click) for this email
        new_events.append({
            'email': email,
            'campaign_id': campaign['id'],
            'campaign_title': campaign['title'],
            'list_id': campaign['list_id'],
            'action': event['action'],
            'timestamp': event['timestamp'],
            'url': event['url'],
            'uuid': str(uuid.uuid1()),
            'workflow_id': campaign.get('workflow_id')
        })

    return new_events


def get_latest_record_timestamp(records, last_updated, time_key):
    """
    Get the last record, whether that's from the existing bookkeeping or a new
    record we just pulled
    """
    if records:
        record_max = max([r[time_key] for r in records])
        if last_updated:
            return max(record_max, last_updated)
        return record_max
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

def handle_subscriber_activity_response(response, stream, c, last_updated, include_sends):
    """
    Iterate through the response of a subscriber activity request from the
    export api, writing records as we hit the batch size
    """
    batched_records = []
    for r in response.iter_lines():
        if r:
            batched_records = batched_records + transform_event(r, c, include_sends)

            # If there are leftover records, write them!
            if len(batched_records) > BATCH_SIZE:
                write_records_and_update_state(
                    c, stream, batched_records, last_updated)

                batched_records = []
    return batched_records

def handle_list_members_response(response, stream, l, last_updated):
    """
    Iterate through the response of a list membership request from the
    export api, writing records as we hit the batch size
    """
    header = None
    batched_records = []
    for r in response.iter_lines():
        if r:
            if header:
                batched_records = batched_records + [dict(
                    zip(header, json.loads(r.decode('utf-8'))))]

                # If there are leftover records, write them!
                if len(batched_records) > BATCH_SIZE:
                    write_records_and_update_state(
                        l, stream, batched_records, last_updated)

                    batched_records = []
            else:
                header = json.loads(r.decode('utf-8'))
    return batched_records

def run_export_request(ctx, entity, stream, last_updated, retries=0):
    """
    This is the main wrapper over Mailchimp's export API. It does the following:
    1. Preps the parameters for the export request depending on the id of the
        campaign, whether this is a backfill and what date we're looking back to
    2. Makes the call to the export API
    3. Processes the response
    4. Handles any errors by retrying
    """
    batched_records = []
    params = {
        'id': entity['id']
    }

    include_sends = False
    params[V3_SINCE_KEY[stream]] = transform_send_time(last_updated[entity['id']])
    if last_updated[entity['id']] == ctx.get_start_date():
        params['include_empty'] = True
        include_sends = True
    if retries < 3:
        try:
            with ctx.client.export_post(
                    stream, entity, last_updated, params
            ) as res:
                if stream in (
                        IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY,
                        IDS.AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY):
                    batched_records = \
                        handle_subscriber_activity_response(
                            res, stream, entity, last_updated, include_sends
                        )
                elif stream == IDS.LIST_MEMBERS:
                    batched_records = handle_list_members_response(
                        res, stream, entity, last_updated
                    )

                if batched_records:
                    write_records_and_update_state(
                        entity, stream, batched_records, last_updated)

        except Exception as e:
            logger.info(e)
            logger.info('Waiting 30 seconds - then retrying')
            time.sleep(30)
            retries += 1
            run_export_request(ctx, entity, stream, last_updated, retries)

    else:
        logger.info('Too many fails for %s, continuing to others' % entity['id'])

def v3_postprocess(records, entity, stream, last_updated):
    """
    These endpoints have intermediate resources that we use (i.e. they aren't
    keyed off Automation Workflow or Campaign, but have to hit another endpoint
    in between). Thus, we need to change their attributes to point at the
    top-level resource.
    """
    processed_records = []
    for record in records:
        if stream == IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES:
            record['workflow_id'] = entity['workflow_id']
            record['workflow_email_id'] = entity['id']
        elif stream == IDS.CAMPAIGN_UNSUBSCRIBES:
            record['variate_id'] = record['campaign_id']
            record['campaign_id'] = entity['id']

        if record[BOOK.return_bookmark_path(stream)[1]] <= \
           last_updated[entity['id']]:
            continue
        processed_records.append(record)
    return processed_records

def run_v3_request(ctx, entity, stream, last_updated, retries=0, offset=0, param_id=None):
    """
    This is the main wrapper over Mailchimp's V3 API. It does more or less the
    same things that the export API does, but it handles list membership and 
    unsubscribe events rather than email activity
    """
    if not param_id:
        param_id = entity['id']
    batched_records = []
    record_key = V3_API_INDEX_NAMES[stream]

    if retries < 20:
        try:
            while True:
                params = {
                    'offset': offset,
                    'count': PAGE_SIZE,
                }
                if V3_SINCE_KEY.get(stream):
                    params[V3_SINCE_KEY[stream]] = transform_send_time(
                        last_updated[entity['id']])

                response = ctx.client.GET(stream, params, item_id=param_id)
                content = json.loads(response.content)
                records = v3_postprocess(content[record_key], entity, stream, last_updated)

                if len(records) == 0:
                    break
                offset += len(records)
                batched_records += records
                if len(batched_records) > BATCH_SIZE:
                    write_records_and_update_state(
                        entity, stream, batched_records, last_updated)

                    batched_records = []

            if batched_records:
                write_records_and_update_state(
                        entity, stream, batched_records, last_updated)

        except Exception as e:
            logger.info(e)
            logger.info('Waiting 30 seconds - then retrying')
            time.sleep(30)
            retries += 1
            run_v3_request(ctx, entity, stream, last_updated, retries, offset)
    else:
        logger.info('Too many fails for %s, continuing to others' % entity['id'])

def call_stream_incremental(ctx, stream):
    last_updated = ctx.get_bookmark(BOOK.return_bookmark_path(stream)) or \
                   defaultdict(str)

    stream_resource = SUB_STREAMS[stream]

    for e in getattr(ctx, stream_resource):
        ctx.update_latest(e['id'], last_updated)

        logger.info('querying {stream_resource} id: {id}, since: {since}, '
            'for: {stream}'.format(
                stream_resource=stream_resource.split('_')[-1][:-1],
                id=e['id'],
                since=last_updated[e['id']],
                stream=stream.split('_')[-1]
            )
        )

        handlers = {
            IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY: run_export_request,
            IDS.LIST_MEMBERS: run_v3_request,
            IDS.CAMPAIGN_UNSUBSCRIBES: run_v3_request,
            IDS.AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY: run_export_request,
            IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES: run_v3_request
        }
        if stream == IDS.CAMPAIGN_UNSUBSCRIBES and e['variate_combination_ids']:
            for combo_id in e['variate_combination_ids']:
                handlers[stream](ctx, e, stream, last_updated, param_id=combo_id)
        else:
            handlers[stream](ctx, e, stream, last_updated)

        ctx.set_bookmark_and_write_state(
            BOOK.return_bookmark_path(stream),
            last_updated)

    return last_updated

def filter_records(ctx, stream, records):
    filtered_records = []

    for record in records:
        if stream == IDS.CAMPAIGNS:
            last_updated = ctx.get_bookmark(BOOK.return_bookmark_path(
                           IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY)) or defaultdict(str)
            if not record['id'] in last_updated or record['send_time'] > \
                                                   ctx.get_lookback_date():
                filtered_records.append(record)
        else:
            filtered_records.append(record)
    return filtered_records

def call_stream_full(ctx, stream, item_id=None):
    records = []
    offset = 0
    content_key = V3_API_INDEX_NAMES.get(stream, stream)
    while True:
        params = {'offset': offset}
        if stream == IDS.CAMPAIGNS:
            params['status'] = 'sent'
        if stream == IDS.AUTOMATION_WORKFLOWS:
            params['status'] = 'sending'

        response = ctx.client.GET(stream, params, item_id)
        content = json.loads(response.content)
        records += content[content_key]
        item_count = content['total_items']
        if len(content[content_key]) == 0:
            break
        offset += len(content[content_key])

    if not item_id:
        write_records(stream, records)

    getattr(ctx, 'save_%s_meta' % stream)(filter_records(ctx, stream, records))

def save_state(ctx, stream, bk):
    ctx.set_bookmark(BOOK.return_bookmark_path(stream), bk)
    ctx.write_state()
