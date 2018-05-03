import requests
from singer import metrics
import backoff

CAMPAIGN_URI = "https://{dc}.api.mailchimp.com/export/1.0/"  # noqa
V3_URI = "https://{dc}.api.mailchimp.com/3.0/{stream}"


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

    def url_v3(self, stream):
        return V3_URI.format(dc=self.dc, stream=stream)

    def create_get_request(self, path, params):
        return requests.Request(method="GET", url=self.url(path),
                                params=params)

    def create_get_request_v3(self, stream, params):
        return requests.Request(method="GET", url=self.url_v3(stream),
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

    def GET_v3(self, stream, params, *args, **kwargs):
        req = self.create_get_request_v3(stream, params)
        return self.request_with_handling(req, *args, **kwargs)

    def put(self, path, entity, last_updated):
        return requests.put(self.url(path),
                            params=self.ctx.get_params(entity['id'],
                                                       last_updated),
                            timeout=15 * 60)
