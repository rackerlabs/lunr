# Copyright (c) 2011-2015 Rackspace US, Inc.
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


from lunr.common.subcommand import SubCommand, SubCommandParser, opt, noargs
from unittest import TestCase


class Api(SubCommand):
    """
    Provides a command line interface to the API

    SubCommandParser can display multi-line comments like this
    in the command description help message
    """

    def __init__(self):
        # let the base class setup methods in our class
        SubCommand.__init__(self)
        # Give our sub command a name
        self._name = 'api'

    @opt('--last-name', help="optional last name of the thingy")
    @opt('name', help="Name of the thingy")
    def create(self, name, last_name=None):
        """ Create a new thingy """
        return (name, last_name)

    @opt('name', help="Name of the thingy")
    @opt('--extra1', help="Extra thingy 1")
    @opt('--extra2', help="Extra thingy 2")
    def arguments(self, name, args):
        """
        if there are more command line arguments than arguments in
        the method and 'args' is in the method declaration the remaining
        args are passed in as a list to 'args'
        """
        return (name, args['extra1'], args['extra2'])

    @opt('--under-score', help="args with dashes '-' get converted")
    def underscore_command(self, under_score=None):
        """
        commands with an underscore '_' in the name get converted
        to a dash '-' on the command line
        """
        return under_score

    @noargs
    def list(self):
        """ No args is ok """
        return "listing"


class TestSubCommands(TestCase):

    def setUp(self):
        self.parser = SubCommandParser([Api()])

    def test_opt_decorator(self):
        result = self.parser.run("api create derrick".split())
        self.assertEquals(result, ('derrick', None))

        result = self.parser.run("api create derrick "
                                 "--last-name wippler".split())
        self.assertEquals(result, ('derrick', 'wippler'))

    def test_extra_arguments(self):
        result = self.parser.run("api arguments derrick --extra1 extra1"
                                 " --extra2 extra2".split())
        self.assertEquals(result, ('derrick', 'extra1', 'extra2'))

    def test_underscore_command(self):
        result = self.parser.run("api underscore-command".split())
        self.assertEquals(result, None)

        result = self.parser.run("api underscore-command --under-score "
                                 "lovesCamelCaseInstead".split())
        self.assertEquals(result, 'lovesCamelCaseInstead')

    def test_noargs(self):
        result = self.parser.run('api list'.split())
        self.assertEquals(result, "listing")
