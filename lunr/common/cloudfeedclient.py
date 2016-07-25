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

from xml.dom import minidom
from urllib2 import Request, urlopen, URLError, HTTPError
from httplib import HTTPException
from lunr.common.exc import HTTPClientError, ClientException
from urllib import urlencode
import socket


class FeedError(ClientException):
    pass


class FeedUnchanged(FeedError):
    pass


class InvalidMarker(FeedError):
    pass


class GetPageFailed(FeedError):
    pass


class Feed(object):
    def __init__(self, conf, logger, feed_url, auth_token,
                 etag=None, read_forward=True, last_event=None):

        self.logger = logger
        self.feed_url = feed_url or conf.string('terminator', 'feed_url', 'none')
        self.timeout = int(conf.string('cloudfeedsclient', 'timeout', 10))
        self.auth_token = auth_token
        self.etag = etag
        self.new_etag = None
        self.processed_events = []
        self.last_event = last_event
        self.read_forward = read_forward
        self.feed_limit = conf.int('cloudfeedsclient', 'feed_limit', 25)

    def get(self, url, **kwargs):
        return self.request(url, method='GET', **kwargs)

    def request(self, url, method, **kwargs):
        headers = {'X-Auth-Token': self.auth_token}
        req = Request(url, data=urlencode(kwargs), headers=headers)
        req.get_method = lambda *args, **kwargs: method

        try:
            return urlopen(req, timeout=self.timeout)
        except (HTTPError, URLError, HTTPException, socket.timeout) as e:
            raise HTTPClientError(req, e)

    def get_page(self, feed_url):
        # GET the feed
        resp = self.get(feed_url)
        # If returned non 200 class response code
        if (resp.getcode() / 100) != 2:
            if resp.getcode() == 404:
                raise InvalidMarker("GET '%s' returned 404; invalid marker"
                                    % feed_url)
            raise GetPageFailed("GET '%s' returned %d when trying to read feed"
                            % (feed_url, resp.getcode()),
                            code=resp.getcode())

        # If we asked for read_forward
        if not self.read_forward:
            # Compare the etag in the response
            self.compare_etag(resp)

        # Read and parse the response
        content = resp.read()
        if not len(content):
            raise GetPageFailed("GET '%s' returned an empty page" % feed_url)
        return minidom.parseString(content)

    def compare_etag(self, resp):
        if self.new_etag is None:
            self.new_etag = resp.info().getheader('etag')
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
        print("URL: %s" % url)
        page = self.get_page(url)

        while page:
            yield page

            if self.read_forward is True:
                url = self.get_url(page, 'previous')
            else:
                url = self.get_url(page, 'next')

            if url:
                print("Last event: %s" % self.last_event)
                print("URL: %s" % url)
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

    @staticmethod
    def get_attributes(element):
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
                data = self.get_attributes(event)
                data['product'] = self.get_attributes(product)
                events.append(data)
            if self.read_forward:
                events.reverse()
            for event in events:
                self.last_event = event['id']
                yield event

    def get_url(self, page, url_key):
        if page:
            for link in self.get_children(page, 'link'):
                if link.getAttribute('rel') == url_key:
                    return link.getAttribute('href')
        return None
