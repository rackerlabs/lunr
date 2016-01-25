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


from argparse import ArgumentError, ArgumentParser
from prettytable import PrettyTable
from collections import namedtuple
from os.path import basename
import inspect
import sys
import re


Option = namedtuple('Option', ['args', 'kwargs'])


def user_input(msg, default=None, validate=None):
    while True:
        input = raw_input(msg)
        if default and input == '':
            return default
        return input


def confirm(msg, default='Y', force=False):
    if force:
        return True
    while True:
        result = user_input(msg + " [Y/n] (%s)? " % default, default)
        if re.match("^(Y|y)$", result):
            return True
        if re.match("^(N|n)$", result):
            return False


def opt(*args, **kwargs):
    def decorator(method):
        if not hasattr(method, 'options'):
            method.options = []
        if args:
            # Append the option to our option list
            method.options.append(Option(args, kwargs))
        # No need to wrap, only adding an attr to the method
        return method
    return decorator


def noargs(method):
    method.options = []
    return method


class SubCommandError(Exception):
    pass


class SubCommandParser(object):

    def __init__(self, sub_commands, desc=None):
        self.sub_commands = self.build_dict(sub_commands)
        self.prog = None
        self.desc = desc

    def build_dict(self, sub_commands):
        result = {}
        for cmd in sub_commands:
            name = getattr(cmd, '_name', None)
            if not name:
                raise SubCommandError(
                    "object '%s' has no attribute '_name'; "
                    "please give your SubCommand class a name" % cmd
                )
            result[name] = cmd
        return result

    def run(self, args=None, prog=None):
        # use sys.argv if not supplied
        if not args:
            args = sys.argv[1:]
        self.prog = prog or basename(sys.argv[0])

        # If completion token found in args
        if '--bash-completion' in args:
            return self.bash_completion(args)

        # If bash completion script requested
        if '--bash-completion-script' in args:
            return self.bash_completion_script(self.prog)

        # Find a subcommand in the arguments
        for index, arg in enumerate(args):
            sub_command_keys = self.sub_commands.keys()
            if arg not in sub_command_keys and \
               arg.rstrip('s') in sub_command_keys:
                print >> sys.stderr, 'WARNING: plural name for console ' \
                        'commands is deprecated!'
                arg = arg.rstrip('s')
            if arg in sub_command_keys:
                # Remove the sub command argument
                args.pop(index)
                # set ref to executed command
                self.command = self.sub_commands[arg]
                # Run the sub-command passing the remaining arguments
                return self.command(args, self.prog)

        # Unable to find a suitable sub-command
        return self.help()

    def bash_completion_script(self, prog):
        print '_%(prog)s() {\n'\
            '  local cur="${COMP_WORDS[COMP_CWORD]}"\n'\
            '  local list=$(%(prog)s --bash-completion $COMP_LINE)\n'\
            '  COMPREPLY=($(compgen -W "$list" $cur))\n'\
            '}\n'\
            'complete -F _%(prog)s %(prog)s\n' % locals()

    def bash_completion(self, args):
        # args = ['--bash-completion', '%prog', 'sub-command', 'command']
        try:
            # If a subcommand is already present
            if args[2] in self.sub_commands.keys():
                # Have the subcommand print out all possible commands
                return self.sub_commands[args[2]].bash_completion()
        except (KeyError, IndexError):
            pass

        # Print out all the possible sub command names
        print ' '.join(self.sub_commands.keys())
        return 0

    def help(self):
        print "Usage: %s <command> [-h]\n" % self.prog
        if self.desc:
            print self.desc + '\n'

        print "Available Commands:"
        for name, command in self.sub_commands.iteritems():
            print "  ", name
            # TODO: Print some help message for the commands?
        return 1


class SubCommand(object):

    def __init__(self):
        # Return a dict of all methods with the options attribute
        self.commands = self.methods_with_opts()
        self.prog = None

    def bash_completion(self):
        print ' '.join(self.commands.keys()),
        return 0

    def remove(self, haystack, needles):
        result = {}
        for key, value in haystack.items():
            if key not in needles:
                result[key] = value
        return result

    def opt(self, *args, **kwargs):
        if not hasattr(self, 'globals'):
            self.globals = []
        self.globals.append(Option(args, kwargs))

    def methods_with_opts(self):
        result = {}
        # get a listing of all the methods
        for name in dir(self):
            if name.startswith('__'):
                continue
            method = getattr(self, name)
            # If the method has an options attribute
            if hasattr(method, 'options'):
                name = re.sub('_', '-', name)
                result[name] = method
        return result

    def __call__(self, args, prog):
        self.prog = prog
        """
        Figure out which command for this sub-command should be run
        then pass the arguments to the commands parser
        """
        for index, arg in enumerate(args):
            # Find a command in the arguments
            if arg in self.commands.keys():
                # Get the method for the command
                method = self.commands[arg]
                # Remove the command from the args
                args.pop(index)
                # Parse the remaining arguments
                args = self.parse_args(method, args)
                # Determine the acceptable arguments
                (kwargs, _globals) = self.acceptable_args(
                    self.get_args(method), args)
                # Attach the global options as class variables
                for key, value in _globals.items():
                    setattr(self, key, value)
                # Call the command with the command
                # line args as method arguments
                return method(**kwargs)

        # Unable to find the command
        return self.help()

    def parse_args(self, method, args):
        # create an argument parser
        self._parser = ArgumentParser(prog=method.__name__,
                                      description=method.__doc__)
        # Add method options to the subparser
        for opt in method.options:
            self._parser.add_argument(*opt.args, **opt.kwargs)
        # Add global options to the subparser

        if hasattr(self, 'globals'):
            for opt in self.globals:
                self._parser.add_argument(*opt.args, **opt.kwargs)

        results = {}
        args = vars(self._parser.parse_args(args))
        # Convert dashes to underscore
        for key, value in args.items():
            results[re.sub('-', '_', key)] = value
        return results

    def help(self):
        print "Usage: %s %s <command> [-h]\n" % (self.prog, self._name)
        if self.__doc__:
            stripped = self.__doc__.strip('\n| ')
            print re.sub(' ' * 4, '', stripped)

        print "\nAvailable Commands:"
        for name, command in self.commands.iteritems():
            print "  ", name
            # print "  ", command.__doc__.strip('\n')
        return 1

    def get_args(self, func):
        """
        Get the arguments of a method and return it as a dictionary with the
        supplied defaults, method arguments with no default are assigned None
        """
        def reverse(iterable):
            if iterable:
                iterable = list(iterable)
                while len(iterable):
                    yield iterable.pop()

        args, varargs, varkw, defaults = inspect.getargspec(func)
        result = {}
        for default in reverse(defaults):
            result[args.pop()] = default

        for arg in reverse(args):
            if arg == 'self':
                continue
            result[arg] = None

        return result

    def global_args(self, args):
        if not hasattr(self, 'globals'):
            return {}
        result = {}
        for opt in self.globals:
            arg_name = ''
            # Find the arg with the longest string
            for arg in opt.args:
                if len(arg) > len(arg_name):
                    arg_name = arg
            # Strip leading '--' and convert '-' to '_'
            arg_name = re.sub('-', '_', arg_name.lstrip('--'))
            result[arg_name] = args[arg_name]
        return result

    def acceptable_args(self, _to, _from):
        _globals = {}
        # Collect arguments that will be
        # passed to the method
        for key, value in _to.items():
            if key in _from:
                _to[key] = _from[key]
                del _from[key]
                # Remove arguments that have no value this allows
                # default values on the method signature to take effect
                if _to[key] is None:
                    del _to[key]

        # If the method has a variable called 'args'
        # then assign any extra the arguments to 'args'
        # NOTE: this is for commands with A TON of arguments
        # that just wanna glob all the args togeather in a dict
        if 'args' in _to:
            _to['args'] = _from
            # Figure out what args are global args
            return (_to, self.global_args(_from))

        # Everything left, will go into the global options
        for key in _from.keys():
            _globals[key] = _from[key]

        return (_to, _globals)


class Displayable(object):
    def display(self, results, headers=None, output=None):
        # No results?b
        if len(results) == 0:
            if output:
                print >>output, "-- Empty --"
            else:
                print "-- Empty --"
            return

        # If data is a single instance
        if not isinstance(results, list):
            # Display the single item and return
            if output:
                print >>output, self.format_item(results, 1, headers)
            else:
                print self.format_item(results, 1, headers)
            return

        # Assume this is a list of items and build a table
        # Get the headers from the first row in the result
        if (not headers) and isinstance(results[0], dict):
            headers = results[0].keys()

        table = PrettyTable(headers)

        # Iterate through the rows
        for row in results:
            # Extract the correct columns for each row
            if isinstance(row, dict):
                table.add_row(self._build_row(row, headers))
            else:
                table.add_row(row)

        # Print the table
        if output:
            print >>output, table
        else:
            print table

    def _build_row(self, row, headers):
        result = []
        for key in headers:
            if key in row:
                result.append(row[key])
            else:
                result.append('')
        return result

    def _longest_len(self, items):
        longest = 0
        for item in items:
            if len(item) > longest:
                longest = len(item)
        return longest

    def format_item(self, item, offset=0, keys=None):
        result = []
        offset = self._longest_len(item.keys()) + offset
        keys = keys or item.keys()

        for key in keys:
            value = item[key]
            if isinstance(value, list):
                for i, sub_value in enumerate(value):
                    sub_value = '\n' +\
                        self.format_item(sub_value, offset) + '\n'
                    result.append(('%' + str(offset) + 's: %s') % (
                        key + ' #%d' % (i + 1), sub_value))
            else:
                if isinstance(value, dict):
                    if len(value):
                        value = '\n' + self.format_item(value, offset) + '\n'
                result.append(('%' + str(offset) + 's: %s') % (key, value))
        return '\n'.join(result)
