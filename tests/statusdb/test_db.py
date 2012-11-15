import os
import unittest
import logbook
from couchdbkit import Server
from scilifelab.db.statusdb import sample_run_metrics

filedir = os.path.abspath(__file__)
LOG = logbook.Logger(__name__)

## FIXME: check for local couchdb installation
class TestDbConnection(unittest.TestCase):
    def setUp(self):
        self.uri = "http://localhost:5984"
        self.examples = {"sample":"2_120924_AC003CCCXX_ACAGTG",
                         "flowcell":"AC003CCCXX",
                         "project":"J.Doe_00_01"}
        self.server = Server()
        self.con = sample_run_metrics(uri=self.uri)
        self.con.set_db(self.server["samples-test"])

    def test_connection(self):
        """Test database connection"""
        print self.con.names()
        self.assertEqual(self.con.uri, self.uri)

    def test_get_flowcell(self):
        """Test getting a flowcell for a given sample"""
        fc = self.con.get_entry(self.examples["sample"], "flowcell")
        self.assertEqual(str(fc), self.examples["flowcell"])

    def test_get_sample_ids(self):
        """Test getting sample ids given flowcell and sample_prj"""
        sample_ids = self.con.get_sample_ids(fc_id=self.examples["flowcell"])
        LOG.info("Number of samples before subsetting: " + str(len(sample_ids)))
        sample_ids = self.con.get_sample_ids(fc_id=self.examples["flowcell"], sample_prj=self.examples["project"])
        LOG.info( "Number of samples after subsetting: " + str(len(sample_ids)))

    def test_get_samples(self):
        """Test getting samples given flowcell and sample_prj."""
        samples = self.con.get_samples(fc_id=self.examples["flowcell"])
        LOG.info("Number of samples before subsetting: " + str(len(samples)))
        samples = self.con.get_samples(fc_id=self.examples["flowcell"], sample_prj=self.examples["project"])
        LOG.info("Number of samples after subsetting: " + str(len(samples)))
                
    def test_get_project_sample_ids(self):
        """Test getting project sample ids"""
        sample_ids = self.con.get_sample_ids(sample_prj=self.examples["project"])
        fc_sample_ids = ["sample1"]
        prj_sample_ids = ["sample1", "sample2"]
        ids = list(set(fc_sample_ids) | set(prj_sample_ids))
        self.assertEqual(ids, prj_sample_ids)
