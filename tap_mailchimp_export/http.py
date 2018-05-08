import requests
from singer import metrics
from .timeout import timeout
import backoff

FULL_URI = "https://{dc}.api.mailchimp.com/3.0/{stream}"
CAMPAIGN_ACTIVITY_URI = "https://{dc}.api.mailchimp.com/3.0/reports/" \
                        "{campaign_id}/email-activity"
LIST_MEMBERS = FULL_URI + "/{list_id}/members"

# Deprecated
CAMPAIGN_URI = "https://{dc}.api.mailchimp.com/export/1.0/"  # noqa

class RateLimitException(Exception):
    pass


class RemoteDisconnected(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config, ctx):
        self.user_agent = config.get("user_agent")
        self.apikey = config.get("apikey")
        self.dc = self.get_dc(config)
        self.session = requests.Session()
        self.ctx = ctx

        self.headers = self.get_headers()

    @staticmethod
    def get_dc(config):
        return config['apikey'].split('-')[1]

    def get_headers(self):
        return {
            'apikey': self.apikey
        }

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent

        request.params['apikey'] = self.apikey
        request.stream = True

        return self.session.send(request.prepare())

    def url(self, path):
        return _join(CAMPAIGN_URI, path).format(dc=self.dc)

    def url_v3(self, stream, item_id):
        if not item_id:
            return FULL_URI.format(dc=self.dc, stream=stream)
        if stream == 'list_members':
            return LIST_MEMBERS.format(
                dc=self.dc, list_id=item_id, stream='lists'
            )
        return CAMPAIGN_ACTIVITY_URI.format(dc=self.dc, campaign_id=item_id)

    def create_get_request(self, path, params):
        return requests.Request(method="GET", url=self.url(path),
                                params=params)

    def create_get_request_v3(self, stream, item_id, params):
        return requests.Request(method="GET", url=self.url_v3(stream, item_id),
                                params=params)

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 503]:
            raise RateLimitException()
        response.raise_for_status()
        return response

    def GET(self, path, params, *args, **kwargs):
        req = self.create_get_request(path, params)
        return self.request_with_handling(req, *args, **kwargs)

    def GET_v3(self, stream, params={}, item_id=None):
        req = self.create_get_request_v3(stream, item_id, params)
        return self.request_with_handling(req, stream)

    def post(self, path, entity, last_updated):
        return requests.post(self.url(path),
                            params=self.ctx.get_params(entity['id'],
                                                       last_updated),
                            # timeout=15 * 60,
                            stream=True)
