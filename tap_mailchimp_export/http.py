import requests
import singer
import re
from singer import metrics
from .timeout import timeout
from .schemas import (
    EXPORT_API_PATH_NAMES, V3_API_PATH_NAMES, V3_API_ENDPOINT_NAMES
)
import backoff


logger = singer.get_logger()


FULL_URI = "https://{dc}.api.mailchimp.com/3.0/{v3_endpoint}"

EXPORT_URI = "https://{dc}.api.mailchimp.com/export/1.0/"  # noqa

class RateLimitException(Exception):
    pass


class RemoteDisconnected(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


trailing_slash = re.compile('.*\/$')


class Client(object):
    def __init__(self, config, ctx):
        self.user_agent = config.get("user_agent")
        self.apikey = config['access_token'] + '-' + config['dc']
        self.dc = self.get_dc(config)
        self.session = requests.Session()
        self.ctx = ctx
        self.headers = self.get_headers()

    @staticmethod
    def get_dc(config):
        return config['dc']

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

    def export_url(self, stream):
        path = EXPORT_API_PATH_NAMES[stream]
        return _join(EXPORT_URI, path).format(dc=self.dc)

    def v3_url(self, stream):
        return FULL_URI.format(
            dc=self.dc, v3_endpoint=V3_API_ENDPOINT_NAMES[stream])

    def v3_endpoint(self, stream, item_id):
        return '{id}/{path}'.format(
            id=item_id,
            path=V3_API_PATH_NAMES[stream]
        )

    def create_get_request(self, stream, params, item_id=None):
        if item_id:
            url = _join(self.v3_url(stream), self.v3_endpoint(stream, item_id))
        else:
            url = self.v3_url(stream)
        return requests.Request(method="GET", url=url, params=params)

    @backoff.on_exception(backoff.expo,
                          (RateLimitException,
                           requests.exceptions.ConnectionError,
                           requests.exceptions.HTTPError),
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

    def GET(self, stream, params, item_id=None):
        req = self.create_get_request(stream, params, item_id)
        return self.request_with_handling(req, stream)

    def export_post(self, stream, entity, last_updated, params):
        params.update(self.headers)
        url = self.export_url(stream)

        # Add trailing slash to prevent request being redirected to http
        # Example, the following URL gets redirected to http://.../: 
        # https://us10.api.mailchimp.com/export/1.0/campaignSubscriberActivity
        if not trailing_slash.match(url):
            url = f'{url}/'

        return requests.post(url,
                            params=params,
                            timeout=30 * 60
                            )
