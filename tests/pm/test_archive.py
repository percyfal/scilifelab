"""
Test analysis subcontroller
"""
import os
import yaml
from cement.core import handler
from test_default import PmTest, PmTestOutputHandler
from scilifelab.pm.core.archive import ArchiveController
from scilifelab.bcbio.flowcell import *

filedir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
runinfo = os.path.join(filedir, "data", "archive", "120829_SN0001_0001_AA001AAAXX", "run_info.yaml")

class PmArchiveTest(PmTest):
    OUTPUT_FILES = []
    
    def test_default(self):
        """Test default archive function"""
        self.app = self.make_app(argv=['archive'])
        handler.register(ArchiveController)
        self._run_app()

    def test_ls(self):
        """Test archive list"""
        self.app = self.make_app(argv=['archive', 'ls'])
        handler.register(ArchiveController)
        self._run_app()
        self.eq(self.app._output_data['stdout'].getvalue(), '120829_SN0001_0001_AA001AAAXX\n120829_SN0001_0002_BB001BBBXX\n120924_SN0002_0003_CC003CCCXX')

    def test_runinfo(self):
        """Test runinfo list"""
        self.app = self.make_app(argv=['archive', 'runinfo', '-f', '120829_SN0001_0001_AA001AAAXX'])
        handler.register(ArchiveController)
        self._run_app()
        info = Flowcell(runinfo)
        self.eq(self.app._output_data['stdout'].getvalue(),str(info))

    def test_runinfo_yaml(self):
        """Test runinfo yaml list"""
        self.app = self.make_app(argv=['archive', 'runinfo', '-f', '120829_SN0001_0001_AA001AAAXX', '--as_yaml'])
        handler.register(ArchiveController)
        self._run_app()
        info = Flowcell(runinfo)
        self.eq(self.app._output_data['stdout'].getvalue(), info.as_yaml())

    def test_runinfo_list_projects(self):
        """Test runinfo list available projects"""
        self.app = self.make_app(argv=['archive', 'runinfo', '-f', '120829_SN0001_0001_AA001AAAXX', '-P'])
        handler.register(ArchiveController)
        self._run_app()
        self.eq(self.app._output_data['stdout'].getvalue(), 'available projects for flowcell 120829_SN0001_0001_AA001AAAXX:\n\tJ.Doe_00_01\n\tJ.Doe_00_02')
