"""
Test runinfo functions
"""
import os
import yaml
import glob
import re
from cement.core import handler
from test_default import PmTest
from scilifelab.bcbio.flowcell import *
import unittest

filedir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
flowcell = "120829_SN0001_0001_AA001AAAXX"
fc_dir = os.path.join(filedir, "data", "production", flowcell)
runinfo = os.path.join(filedir, "data", "archive", flowcell, "run_info.yaml")
flowcell_casava = "120924_SN0002_0003_CC003CCCXX"
fc_dir_casava = os.path.join(filedir, "data", "production", flowcell_casava)
samplesheet = os.path.join(filedir, "data", "archive", flowcell_casava, "C003CCCXX.csv")


class FlowcellTest(PmTest):
    """Test flowcell object functionality"""
    def test_get_flowcell(self):
        """Read contents of runinfo file and generate flowcell object"""
        fc = Flowcell(runinfo)
        print "\n", fc
        newfc=fc.subset("sample_prj", "J.Doe_00_01")
        self.eq(len(fc), 11)
        self.eq(len(newfc), 7)
        self.eq(fc.projects(), ['J.Doe_00_01', 'J.Doe_00_02'])
        self.eq(newfc.projects(), ['J.Doe_00_01'])
        self.eq(os.path.dirname(fc.filename), os.path.abspath(os.path.dirname(runinfo)))
        self.eq(fc.filename, os.path.abspath(runinfo))
        
    def test_load_flowcell(self):
        """Create object and load run information"""
        self.app = self.make_app(argv = [])
        self.app.setup()
        fc = Flowcell()
        fc.load([os.path.join(self.app.config.get("production", "root"), flowcell),
                 os.path.join(self.app.config.get("archive", "root"), flowcell)])
        self.eq(len(fc), 11)
        self.eq(os.path.dirname(fc.filename), os.path.join(self.app.config.get("archive", "root"), flowcell))
        self.eq(fc.filename, os.path.join(self.app.config.get("archive", "root"), flowcell, "run_info.yaml"))

    def test_getters(self):
        """Test getters"""
        fc = Flowcell(runinfo)
        self.eq(fc.barcodes('2'), [5,7,17,19])
        self.eq(fc.lanes(), ['1','2'])

    def test_barcode_mapping(self):
        """Test barcode mappings"""
        fc = Flowcell(runinfo)
        print fc
        self.eq(fc.barcode_id_to_name('1'), {1: 'P1_101F_index1', 2: 'P1_102F_index2', 3: 'P1_103_index3', 4: 'P1_104F_index4', 8: 'P1_105F_index5', 10: 'P1_106F_index6', 12: 'P1_107_index7'})
        self.eq(fc.barcode_name_to_id('1'), {'P1_102F_index2': 2, 'P1_107_index7': 12, 'P1_106F_index6': 10, 'P1_105F_index5': 8, 'P1_101F_index1': 1, 'P1_103_index3': 3, 'P1_104F_index4': 4})
        self.eq(fc.barcode_name_to_sequence('2'), {'P2_101_index19a': 'ATCACG', 'P2_104_index4a': 'TGACCA', 'P2_102_index12a': 'CGATGT', 'P2_103_index3a': 'TTAGGC'})
        self.eq(fc.barcode_sequence_to_name('2'), {'TGACCA': 'P2_104_index4a', 'CGATGT': 'P2_102_index12a', 'TTAGGC': 'P2_103_index3a', 'ATCACG': 'P2_101_index19a'})
        self.eq(fc.barcode_sequences('2'), ['ATCACG', 'CGATGT', 'TTAGGC', 'TGACCA'])

    def test_glob_str(self):
        """Test construction of glob prefixes"""
        fc = Flowcell(runinfo)
        glob_pfx_str = fc.glob_pfx_str()
        print glob_pfx_str
        self.app = self.make_app(argv = [])
        self.app.setup()
        glob_str = os.path.join(self.app.config.get("production", "root"), flowcell, glob_pfx_str[0])
        print glob_str
                
    def test_collect_files(self):
        """Test getting files"""
        fc = Flowcell(runinfo)
        fc.collect_files(fc_dir)
        print fc.as_yaml()
        fc_new = fc.collect_files(fc_dir, project="J.Doe_00_01")
        print fc_new.as_yaml()
        print fc_new
        print fc
        print fc.lane_files

    def test_unique_lanes(self):
        """Test that flowcell returns object with unique lanes"""
        fc = Flowcell(runinfo)
        new_fc = fc.fc_with_unique_lanes()
        print fc.data
        print fc.as_yaml()
        print new_fc.data
        print new_fc.as_yaml()


    def test_get_flowcell_csv(self):
        """Test to load a flowcell as a csv"""
        fc = Flowcell(samplesheet)
        print fc
        print fc.data
        print fc.as_yaml()

    def test_get_flowcell_csv_wo_fastq(self):
        """Test to load a flowcell as csv when no fastq information.
        
        For files without fastq info conversion to yaml fails.
        """
        pass
