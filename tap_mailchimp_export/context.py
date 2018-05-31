from datetime import datetime, date, timedelta
import pendulum
import singer
from singer import bookmarks as bks_
from mailsnake import MailSnake
from .http import Client
from .schemas import IDS

def convert_to_mc_date(iso_string):
    return pendulum.parse(iso_string).to_datetime_string()

class Context(object):
    """Represents a collection of global objects necessary for performing
    discovery or for running syncs. Notably, it contains

    - config  - The JSON structure from the config.json argument
    - state   - The mutable state dict that is shared among streams
    - client  - An HTTP client object for interacting with the API
    - catalog - A singer.catalog.Catalog. Note this will be None during
                discovery.
    """
    def __init__(self, config, state):
        self.config = config
        self.state = state
        self.mailsnake = MailSnake(config['apikey'])
        self.client = Client(config, self)
        self.export_client = MailSnake(config['apikey'],
                                       api='export',
                                       requests_opts={'stream': True})
        self._catalog = None
        self.campaigns = []
        self.lists = []
        self.selected_stream_ids = None
        self.now = datetime.utcnow()
        self.lookback_days = config.get('lookback_days')

    @property
    def catalog(self):
        return self._catalog

    @catalog.setter
    def catalog(self, catalog):
        self._catalog = catalog
        self.selected_stream_ids = set(
            [s.tap_stream_id for s in catalog.streams
             if s.is_selected()]
        )

    def get_bookmark(self, path):
        return bks_.get_bookmark(self.state, *path)

    def set_bookmark_and_write_state(self, path, val):
        self.set_bookmark(path, val)
        self.write_state()

    def update_latest(self, id, last_updated):
        if not last_updated.get(id):
            last_updated[id] = self.get_start_date()

    def set_bookmark(self, path, val):
        if isinstance(val, date):
            val = val.isoformat()
        if isinstance(val, str):
            val = pendulum.parse(val).to_iso8601_string()
        bks_.write_bookmark(self.state, path[0], path[1], val)

    def get_offset(self, path):
        off = bks_.get_offset(self.state, path[0])
        return (off or {}).get(path[1])

    def set_offset(self, path, val):
        bks_.set_offset(self.state, path[0], path[1], val)

    def clear_offsets(self, tap_stream_id):
        bks_.clear_offset(self.state, tap_stream_id)

    def update_start_date_bookmark(self, path):
        val = self.get_bookmark(path)
        if not val:
            val = self.get_start_date()
            self.set_bookmark(path, val)
        return pendulum.parse(val)

    def get_start_date(self):
        if self.lookback_days:
            new_date = self.now - timedelta(days=self.lookback_days)
            return convert_to_mc_date(new_date.strftime("%Y-%m-%d"))

    def write_state(self):
        singer.write_state(self.state)

    def save_campaigns_meta(self, campaigns):
        setattr(self, IDS.CAMPAIGNS, [
            {
                'id': c['id'],
                'title': c['settings']['title'],
                'list_id': c['recipients']['list_id'],
                'sent_at': c['send_time'],
            } for c in campaigns
        ])

    def save_lists_meta(self, lists):
        setattr(self, IDS.LISTS, [
            {
                'id': l['id'],
                'name': l['name'],
            } for l in lists
        ])

    def save_automation_workflows_meta(self, workflows):
        setattr(self, IDS.AUTOMATION_WORKFLOWS, [
            {
                'id': w['id'],
                'title': w['settings']['title'],
                'list_id': w['recipients']['list_id'],
                'sent_at': w['start_time']
            } for w in workflows
        ])

    def save_automation_workflow_emails_meta(self, emails):
        setattr(self, IDS.AUTOMATION_WORKFLOW_EMAILS, [
            {
                'id': e['id'],
                'title': e['settings']['title'],
                'list_id': e['recipients']['list_id'],
                'started_at': e['start_time'],
                'automation': True
            } for e in emails
        ])
