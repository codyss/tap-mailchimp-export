#!/usr/bin/env python3
import os
import singer
from singer import utils


class IDS(object):
    CAMPAIGNS = "campaigns"
    CAMPAIGN_SUBSCRIBER_ACTIVITY = "campaign_subscriber_activity"
    CAMPAIGN_UNSUBSCRIBES = "campaign_unsubscribes"
    LISTS = "lists"
    LIST_MEMBERS = "list_members"
    AUTOMATION_WORKFLOWS = "automation_workflows"
    AUTOMATION_WORKFLOW_EMAILS = "automation_workflow_emails"
    AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY = "automation_workflow_subscriber_activity"
    AUTOMATION_WORKFLOW_UNSUBSCRIBES = "automation_workflow_unsubscribes"

stream_ids = [getattr(IDS, x) for x in dir(IDS)
              if not x.startswith("__")]

PK_FIELDS = {
    IDS.CAMPAIGNS: ["id"],
    IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY: ["uuid"],
    IDS.LISTS: ["id"],
    IDS.LIST_MEMBERS: ["email"],
    IDS.CAMPAIGN_UNSUBSCRIBES: ["email"],
    IDS.AUTOMATION_WORKFLOWS: ["id"],
    IDS.AUTOMATION_WORKFLOW_EMAILS: ["id"],
    IDS.AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY: ["email"],
    IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES: ["email"]
}

EXPORT_API_PATH_NAMES = {
    IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY: "campaignSubscriberActivity",
    IDS.LIST_MEMBERS: "list",
    IDS.AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY: "campaignSubscriberActivity"
}

V3_API_PATH_NAMES = {
    IDS.LIST_MEMBERS: "members",
    IDS.CAMPAIGN_UNSUBSCRIBES: "unsubscribed",
    IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES: "unsubscribed",
    IDS.AUTOMATION_WORKFLOW_EMAILS: "emails"
}

V3_API_INDEX_NAMES = {
    IDS.LIST_MEMBERS: "members",
    IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES: "unsubscribes",
    IDS.CAMPAIGN_UNSUBSCRIBES: "unsubscribes",
    IDS.AUTOMATION_WORKFLOWS: "automations",
    IDS.AUTOMATION_WORKFLOW_EMAILS: "emails"
}

V3_API_ENDPOINT_NAMES = {
    IDS.CAMPAIGNS: "campaigns",
    IDS.LISTS: "lists",
    IDS.LIST_MEMBERS: "lists",
    IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES: "reports",
    IDS.CAMPAIGN_UNSUBSCRIBES: "reports",
    IDS.AUTOMATION_WORKFLOWS: "automations",
    IDS.AUTOMATION_WORKFLOW_EMAILS: "automations"
}

V3_SINCE_KEY = {
    IDS.CAMPAIGNS: "since_send_time",
    IDS.LIST_MEMBERS: "since_last_changed",
    IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY: "since",
    IDS.LISTS: "since_date_created",
    IDS.AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY: "since"
}

SUB_STREAMS = {
    IDS.AUTOMATION_WORKFLOW_SUBSCRIBER_ACTIVITY: IDS.AUTOMATION_WORKFLOW_EMAILS,
    IDS.CAMPAIGN_SUBSCRIBER_ACTIVITY: IDS.CAMPAIGNS,
    IDS.CAMPAIGN_UNSUBSCRIBES: IDS.CAMPAIGNS,
    IDS.AUTOMATION_WORKFLOW_UNSUBSCRIBES: IDS.AUTOMATION_WORKFLOW_EMAILS,
    IDS.LIST_MEMBERS: IDS.LISTS
}

INTERMEDIATE_STREAMS = {
    IDS.AUTOMATION_WORKFLOWS: IDS.AUTOMATION_WORKFLOW_EMAILS
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    return utils.load_json(get_abs_path(path))


def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
