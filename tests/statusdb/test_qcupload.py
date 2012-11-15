import os
import unittest
import ConfigParser
import logbook
from scilifelab.db.statusdb import SampleRunMetricsConnection
from scilifelab.bcbio.qc import FlowcellRunMetrics, SampleRunMetrics

filedir = os.path.abspath(__file__)
LOG = logbook.Logger(__name__)

config = ConfigParser.ConfigParser()
if os.path.exists(os.path.join(os.getenv("HOME"), "dbcon.ini")):
    config.readfp(open(os.path.join(os.getenv("HOME"), "dbcon.ini")))

@unittest.skipIf(not os.path.exists(os.path.join(os.getenv("HOME"), "dbcon.ini")), "no {}/dbcon.ini file; skipping test".format(os.getenv("HOME")))
@unittest.skipIf(not config.has_section("statusdb"), "no statusdb config section: skipping")
class TestQCUpload(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(os.path.join(os.getenv("HOME"), "dbcon.ini")):
            self.url = None
            self.user = None
            self.pw = None
            self.examples = {}
            LOG.warning("No such file {}; will not run database connection tests".format(os.path.join(os.getenv("HOME"), "dbcon.ini")))
        else:
            self.url = config.get("couchdb", "url")
            self.user = config.get("couchdb", "username")
            self.pw = config.get("couchdb", "password")
            self.demuxstats = config.get("statusdb", "demuxstats")
            self.sample_kw = {"path":config.get("statusdb", "fcdir"),
                             "flowcell":config.get("statusdb", "fc_name"),
                             "date":config.get("statusdb", "date"),
                             "lane":config.get("statusdb", "lane"),
                             "barcode_name":config.get("statusdb", "name"),
                             "sample_prj":config.get("statusdb", "project"),
                             "barcode_id":config.get("statusdb", "barcode_id"),
                             "sequence":config.get("statusdb", "sequence")}
            self.fc_kw = {"path":config.get("statusdb", "fcdir"),
                          "fc_date":config.get("statusdb", "date"),
                          "fc_name":config.get("statusdb", "fc_name")}
            self.examples = {"sample":config.get("examples", "sample"),
                             "flowcell":config.get("examples", "flowcell"),
                             "project":config.get("examples", "project")}
            self.fcrm = FlowcellRunMetrics(**self.fc_kw)
            self.srm  = SampleRunMetrics(**self.sample_kw)

    def test_demuxstats(self):
        """Test reading demultiplex statistics"""
        if not self.examples:
            LOG.info("Not running test")
            return
        metrics = self.fcrm.parse_demultiplex_stats_htm()
        print metrics["Barcode_lane_statistics"][0]

    def test_map_srmseqid_to_srmid(self):
        """Map srm seq id names to srm ids"""
        if not self.examples:
            LOG.info("Not running test")
            return
        sample_con = SampleRunMetricsConnection(username=self.user, password=self.pw, url=self.url)
        sample_map = {}
        for k in sample_con.db:
            obj = sample_con.db.get(k)
            sample_seq_id = "{}_{}_{}_{}".format(obj.get("lane"), obj.get("date"), obj.get("flowcell"), obj.get("sequence", "NoIndex"))
            if not sample_seq_id in sample_map.keys():
                sample_map[sample_seq_id] = [k]
            else:
                LOG.warn("duplicate for {}".format(sample_seq_id))
                sample_map[sample_seq_id].append(k)
        for k,v in sample_map.items():
            if len(v) > 1:
                LOG.info(k, v)
