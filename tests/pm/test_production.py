"""
Test production subcontroller
"""

import os
import yaml
import shutil
import glob
from cement.core import handler
from cement.utils import shell
from test_default import PmTest
from scilifelab.pm.core.production import ProductionController
from scilifelab.utils.misc import walk

filedir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
delivery_dir = os.path.abspath(os.path.join(filedir, "data", "projects", "j_doe_00_01", "data"))
intermediate_delivery_dir = os.path.abspath(os.path.join(filedir, "data", "projects", "j_doe_00_01", "intermediate"))

class PmProductionTest(PmTest):
    def setUp(self):
        super(PmProductionTest, self).setUp()
        if os.path.exists(delivery_dir):
            flist = walk(os.path.join(delivery_dir))
            for x in flist:
                os.unlink(x)
            shutil.rmtree(delivery_dir)
        
    def test_ls(self):
        self.app = self.make_app(argv = ['production', 'ls'])
        handler.register(ProductionController)
        self._run_app()
        self.eq(self.app._output_data['stdout'].getvalue(), '120829_SN0001_0001_AA001AAAXX\n120829_SN0001_0002_BB001BBBXX\n120924_SN0002_0003_CC003CCCXX\nJ.Doe_00_04\nJ.Doe_00_05\nJ.Doe_00_06')

    def test_from_pre_to_casava_transfer(self):
        """Test from pre-casava to casava transfer to project directory"""
        self.app = self.make_app(argv = ['production', 'transfer', 'J.Doe_00_01', '-f', '120829_SN0001_0001_AA001AAAXX', '--from_pre_casava'])
        handler.register(ProductionController)
        self._run_app()
        res = shell.exec_cmd(["ls", "-1", os.path.join(delivery_dir, "P1_101F_index1", "120829_AA001AAAXX")])
        self.eq(set(['1_120829_AA001AAAXX_barcode', '1_120829_AA001AAAXX_nophix_1-sort-dup.align_metrics', '1_120829_AA001AAAXX_nophix_1-sort-dup.bam', '1_120829_AA001AAAXX_nophix_1-sort-dup.dup_metrics', '1_120829_AA001AAAXX_nophix_1-sort-dup.hs_metrics', '1_120829_AA001AAAXX_nophix_1-sort-dup.insert_metrics', '1_120829_AA001AAAXX_nophix_1-sort.bam', 'P1_101F_index1-bcbb-pm-config.yaml', 'alignments']),set(list(sorted(res[0].split()))))
    
    def test_from_pre_to_pre_casava_transfer(self):
        """Test pre_casava transfer to project directory"""
        self.app = self.make_app(argv = ['production', 'transfer', 'J.Doe_00_01', '-f', '120829_SN0001_0001_AA001AAAXX', '--from_pre_casava', '--to_pre_casava', '--quiet'])
        handler.register(ProductionController)
        self._run_app()
        ## Assert data output
        res = shell.exec_cmd(["ls", "-1", os.path.join(delivery_dir, "120829_AA001AAAXX", "1_120829_AA001AAAXX_barcode")])
        res_files = ['1_120829_AA001AAAXX_nophix_10_1_fastq.txt', '1_120829_AA001AAAXX_nophix_10_2_fastq.txt',
                     '1_120829_AA001AAAXX_nophix_12_1_fastq.txt','1_120829_AA001AAAXX_nophix_12_2_fastq.txt',
                     '1_120829_AA001AAAXX_nophix_1_1_fastq.txt','1_120829_AA001AAAXX_nophix_1_2_fastq.txt',
                     '1_120829_AA001AAAXX_nophix_2_1_fastq.txt','1_120829_AA001AAAXX_nophix_2_2_fastq.txt',
                     '1_120829_AA001AAAXX_nophix_3_1_fastq.txt','1_120829_AA001AAAXX_nophix_3_2_fastq.txt',
                     '1_120829_AA001AAAXX_nophix_4_1_fastq.txt','1_120829_AA001AAAXX_nophix_4_2_fastq.txt',
                     '1_120829_AA001AAAXX_nophix_8_1_fastq.txt','1_120829_AA001AAAXX_nophix_8_2_fastq.txt']
        self.eq(set(res_files), set(res[0].split()))
        ## Assert intermediate delivery output 
        res = [os.path.basename(x) for x in glob.glob(os.path.join(intermediate_delivery_dir, "120829_AA001AAAXX", "*_10*" ))]
        self.eq(set(['1_120829_AA001AAAXX_nophix_10-sort.bam', '1_120829_AA001AAAXX_nophix_10-sort-dup.align_metrics', '1_120829_AA001AAAXX_nophix_10-sort-dup.dup_metrics', '1_120829_AA001AAAXX_nophix_10-sort-dup.hs_metrics', '1_120829_AA001AAAXX_nophix_10-sort-dup.insert_metrics', '1_120829_AA001AAAXX_nophix_10-sort-dup.bam']), set(res))
        res = [os.path.basename(x) for x in glob.glob(os.path.join(intermediate_delivery_dir, "120829_AA001AAAXX", "*_8*" ))]
        self.eq(set(['1_120829_AA001AAAXX_nophix_8-sort.bam', '1_120829_AA001AAAXX_nophix_8-sort-dup.align_metrics', '1_120829_AA001AAAXX_nophix_8-sort-dup.dup_metrics', '1_120829_AA001AAAXX_nophix_8-sort-dup.hs_metrics', '1_120829_AA001AAAXX_nophix_8-sort-dup.insert_metrics', '1_120829_AA001AAAXX_nophix_8-sort-dup.bam']), set(res))
        ## Assert pruned yaml file contents
        with open(os.path.join(delivery_dir, "120829_AA001AAAXX", "project_run_info.yaml")) as fh:
            runinfo_yaml = yaml.load(fh)
        self.eq(runinfo_yaml['details'][0]['multiplex'][0]['name'], 'P1_101F_index1')
        self.eq(runinfo_yaml['details'][0]['multiplex'][0]['description'], 'J.Doe_00_01_P1_101F_index1')
        self.eq(set(runinfo_yaml['details'][0]['multiplex'][0]['files']), set([os.path.join(delivery_dir,"120829_AA001AAAXX", "1_120829_AA001AAAXX_barcode", os.path.basename(x)) for x in ['1_120829_AA001AAAXX_nophix_1_1_fastq.txt','1_120829_AA001AAAXX_nophix_1_2_fastq.txt']]))

    def test_quiet(self):
        """Test pre_casava delivery to project directory with quiet flag"""
        self.app = self.make_app(argv = ['production', 'transfer',  'J.Doe_00_01', '-f', '120829_SN0001_0001_AA001AAAXX', '--from_pre_casava', '--to_pre_casava', '--quiet'])
        handler.register(ProductionController)
        self._run_app()

    def test_from_casava_to_casava_transfer(self):
        """Test from casava to casava transfer to project directory"""
        self.app = self.make_app(argv = ['production', 'transfer', 'J.Doe_00_04'])
        handler.register(ProductionController)
        self._run_app()

    def test_from_casava_to_casava_custom_transfer(self):
        """Test from casava to casava transfer to custom project directory"""
        self.app = self.make_app(argv = ['production', 'transfer', 'J.Doe_00_04', '--transfer_dir', 'j_doe_00_04_custom'])
        handler.register(ProductionController)
        self._run_app()
        delivery_dir = os.path.abspath(os.path.join(filedir, "data", "projects", "j_doe_00_04_custom", "data"))
        with open(os.path.join(delivery_dir, "P001_101_index3", "120924_CC003CCCXX", "P001_101_index3-bcbb-config.yaml")) as fh:
            runinfo_yaml = yaml.load(fh)
        res = shell.exec_cmd(["ls", "-1", os.path.join(delivery_dir,  "P001_101_index3", "120924_CC003CCCXX")])
        self.eq(len(set(res[0].split())), 22)

        
