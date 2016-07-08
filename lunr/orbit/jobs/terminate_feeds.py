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

# import cloudfeedclient
# from lunr.orbit import CronJob
# from lunr.common.config import LunrConfig
# from sqlalchemy import create_engine,
# from sqlalchemy.orm import sessionmaker
import ConfigParser
import requests

# Base = declarative_base()

# class TerminateFeed(CronJob):
class TerminateFeed():

    def run(self):
        config = ConfigParser.ConfigParser()
        config.read('/etc/lunr/orbit.conf')
        url = config.get('terminator', 'dburl')
        # engine = create_engine(url, echo=True, pool_recycle=3600)
        headers = {'Content-type':'application/json'}
        payload = {"auth":{"RAX-KSKEY:apiKeyCredentials": {"username":"terminator","apiKey":"APIKEY"}}}
        token = requests.post(
            "http://localhost:6000/v2.0/tokens",
            json=payload, headers=headers)
        print token.json()

if __name__ == '__main__':
    terminator = TerminateFeed()
    terminator.run()

