# Copyright (c) 2011-2016 Rackspace US, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from eventlet import timeout
from httplib import HTTPConnection, HTTPSConnection, urlsplit
from xml.dom import minidom


class FeedUnchanged(Exception):
    pass


class ReachedCurrentPage(Exception):
    pass


class InvalidMarker(Exception):
    pass


class MaxEventsErroring(Exception):
    pass


class UnableToGetFeedPage(Exception):
    pass


class Feed(object):
    def __init__(self, conf, logger, etag, feed_url, auth_token,
                 read_forward=False, last_event=None):

        self.logger = logger
        self.feed_url = feed_url
        self.timeout_seconds = int(conf.get('timeout_seconds', 10))
        self.auth_token = auth_token
        self.etag = etag
        self.new_etag = None
        self.processed_events = []
        self.last_event = last_event
        self.read_forward = read_forward
        self.feed_limit = conf.get('feed_limit', '25')

    def get_connection(self, url):
        with timeout.Timeout(self.timeout_seconds):
            scheme, netloc, path, query, fragment = urlsplit(url)
            if scheme == 'https':
                Connection = HTTPSConnection
            else:
                Connection = HTTPConnection
            return Connection(netloc)

    def get_page(self, feed_url):
        try_count = 0
        while try_count < 5:
            try_count += 1
            try:
                conn = self.get_connection(feed_url)
                headers = {'x-auth-token': self.auth_token}
                with timeout.Timeout(self.timeout_seconds):
                    conn.request('GET', feed_url, '', headers)
                    resp = conn.getresponse()
            except timeout.Timeout:
                self.logger.info('Timed out getting page')
            else:
                if resp.status // 100 == 2:
                    if not self.read_forward:
                        self.compare_etag(resp)
                    content = resp.read()
                    if content:
                        return minidom.parseString(content)
                    else:
                        self.logger.error('Feed returned an empty page')
                elif resp.status == 401:
                    self.logger.error('Feed request returned 401')
                elif resp.status == 404:
                    self.logger.error('Feed request returned 404 ' +
                                      'for marker: %s' % self.last_event)
                    raise InvalidMarker()
                else:
                    self.logger.error(
                        'Got status %d when trying to read feed' %
                        resp.status)
        raise UnableToGetFeedPage()

    def compare_etag(self, resp):
        if self.new_etag is None:
            headers = \
                dict((h.lower(), v) for h, v in resp.getheaders())
            self.new_etag = headers.get('etag')
            if self.new_etag and self.new_etag == self.etag:
                raise FeedUnchanged()

    def get_pages(self):
        url = self.feed_url
        if self.read_forward:
            if self.last_event:
                url = '%s?marker=urn:uuid:%s&' % (url, self.last_event) + \
                      'direction=forward&limit=%s' % self.feed_limit
            else:
                url = '%s?marker=last' % (url)

        page = self.get_page(url)
        while page:
            yield page

            if self.read_forward is True:
                url = self.get_url(page, 'previous')
            else:
                url = self.get_url(page, 'next')

            if url:
                page = self.get_page(url)
            else:
                page = None

    def get_child(self, element, tag_name):
        children = list(self.get_children(element, tag_name))
        if len(children) == 0 or len(children) > 1:
            raise ValueError()
        return children[0]

    def get_children(self, element, tag_name):
        for child in element.childNodes:
            if child.localName == tag_name:
                yield child
            for c in self.get_children(child, tag_name):
                yield c

    def get_attributes(self, element):
        data = dict()
        for attr in element.attributes.keys():
            data[attr] = element.getAttribute(attr)
        return data

    def get_events(self):
        for page in self.get_pages():
            events = list()
            for entry in self.get_children(page, 'entry'):
                event = self.get_child(entry, 'event')
                product = self.get_child(event, 'product')
                data = self.xget_attributes(event)
                data['product'] = self.get_attributes(product)
                events.append(data)
            if self.read_forward:
                events.reverse()
            for event in events:
                yield event

    def get_url(self, page, url_key):
        if page:
            for link in self.get_children(page, 'link'):
                if link.getAttribute('rel') == url_key:
                    return link.getAttribute('href')
        return None
