import os
import unittest
import logbook
import pandas as pd
from couchdbkit import Server
from pandas.core.format import set_eng_float_format
from scilifelab.db.statusdb import project_summary
from ..classes import SciLifeTest

filedir = os.path.abspath(__file__)
LOG = logbook.Logger(__name__)


class TestQCData(SciLifeTest):
    def setUp(self):
        self.uri = "http://localhost:5984"
        self.examples = {"sample":"2_120924_AC003CCCXX_ACAGTG",
                         "flowcell":"AC003CCCXX",
                         "project":"J.Doe_00_01"}
        self.server = Server()
        self.con = project_summary(uri=self.uri)
        self.con.set_db(self.server["projects-test"])

    def tearDown(self):
        pass

    def test_qc(self):
        if not self.examples:
            LOG.info("Not running test")
            return
        qcdata = self.con.get_qc_data(self.examples["project"], self.examples["flowcell"], sample_db="samples-test")
        print qcdata
        qcdf = pd.DataFrame(qcdata)
        print qcdf
        set_eng_float_format(accuracy=1, use_eng_prefix=True)
        qcdf.ix["TOTAL_READS"] = qcdf.ix["TOTAL_READS"] 
        qcdft = qcdf.T
        print qcdft.to_string()

