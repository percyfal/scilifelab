import os
import sys
import csv
import yaml
from couchdbkit import Server
from couchdbkit.designer import pushapps
import unittest
import time
import logbook
import socket
from itertools import izip

from classes import PmFullTest
from scilifelab.pm.ext.ext_qc import update_fn
from scilifelab.db.statusdb import sample_run_metrics, flowcell_run_metrics, project_summary, calc_avg_qv, equal
from scilifelab.pm.bcbio.utils import fc_id, fc_parts, fc_fullname
from scilifelab.utils.timestamp import utc_time

filedir = os.path.dirname(os.path.abspath(__file__))
dirs = {'production': os.path.join(filedir, "data", "production")}

LOG = logbook.Logger(__name__)

flowcells = ["120924_SN0002_0003_AC003CCCXX", "121015_SN0001_0002_BB002BBBXX"]
flowcell_dir = os.path.join(filedir, "data", "archive")
projects = ["J.Doe_00_01", "J.Doe_00_02", "J.Doe_00_03"]
project_dir = os.path.join(filedir, "data", "production")
design_dir = os.path.join(filedir, os.pardir, os.pardir, "scilifelab", "db", "_design")

## Try connecting to server
has_couchdb = True
try:
    server = Server()
    server.info()
except socket.error as e:
    has_couchdb = False
    LOG.info("To run db tests setup a local couchdb server at http://localhost:5984")
    time.sleep(3)
    pass
    
DATABASES = ["samples-test", "projects-test", "flowcells-test"]
@unittest.skipIf(not has_couchdb, "No couchdb server running in http://localhost:5984")
class TestCouchDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Create test databases in local server"""
        if not has_couchdb:
            return
        server = Server()
        for dbase in DATABASES:
            if not dbase in server:
                server.create_db(dbase)
            db = server[dbase]
            dbname = dbase.replace("-test", "")
            design = os.path.join(design_dir, dbname)
            pushapps(os.path.join(design), db)

        ## Upload projects
        with open(os.path.join(filedir, "data", "config", "project_summary.yaml")) as fh:
            prj_summary = yaml.load(fh)
        db = server.get_db("projects-test")
        project_summary.set_db(db)
        for p in prj_summary:
            prj = project_summary(p)
            prj.entity_type = prj.doc_type
            prj.creation_time = utc_time()
            if p.get("project_id") in project_summary.names().keys():
                db_prj = project_summary.get(project_summary.names()[p.get("project_id")])
                prj._id = db_prj._id
                prj.creation_time = db_prj.creation_time
                if equal(db_prj, prj):
                    pass
                else:
                    prj.modification_time = utc_time()
                    prj.save(force_update=True)
            else:
                prj.save()

    # @classmethod
    # def tearDownClass(cls):
    #     db = couchdb.Server()
    #     for x in DATABASES:
    #         LOG.info("Deleting database {}".format(x))
    #         del db[x]

    def test_srm(self):
        srm = sample_run_metrics()
        server = Server()
        db = server.get_or_create_db("samples-test")
        sample_run_metrics.set_db(db)
        for n in sample_run_metrics.view("names/name"):
            s = sample_run_metrics.get(n["id"])
            print s.calc_avg_qv()
            print calc_avg_qv(s)
