"""
Project Management Tools
"""

__import__('pkg_resources').declare_namespace(__name__)

import os
import sys
import re
import argparse
import textwrap
import subprocess
from cStringIO import StringIO

from cement.core import foundation, controller, handler, backend, output, hook

from scilifelab.pm.core import command
from scilifelab.pm.core import shell
from scilifelab.pm.core.controller import PmController
from scilifelab.pm.core.log import PmLogHandler

LOG = backend.minimal_logger(__name__)    

class PmApp(foundation.CementApp):
    """
    Main Pm application.

    """
    class Meta:
        label = "pm"
        base_controller = PmController
        cmd_handler = shell.ShCommandHandler
        log_handler = PmLogHandler

    def __init__(self, label=None, **kw):
        super(PmApp, self).__init__(**kw)
        handler.define(command.ICommand)
        self.cmd = None

    def setup(self):
        super(PmApp, self).setup()
        self._setup_cmd_handler()
        ## FIXME: look at backend in cement
        self._output_data = dict(stdout=StringIO(), stderr=StringIO(), debug=StringIO())

    def _setup_cmd_handler(self):
        """Setup a command handler"""
        LOG.debug("setting up {}.command handler".format(self._meta.label))
        self.cmd = self._resolve_handler('command', self._meta.cmd_handler)

    def flush(self):
        """Flush output contained in _output_data dictionary"""
        if self._output_data["stdout"].getvalue():
            print self._output_data["stdout"].getvalue()
        if self._output_data["stderr"].getvalue():
            print >> sys.stderr, self._output_data["stderr"].getvalue()
