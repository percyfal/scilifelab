"""Casava delivery module"""
import os
from datetime import datetime

import scilifelab.log

LOG = scilifelab.log.minimal_logger(__name__)

def casava_delivery(project_id, flowcell_id, transfer_fun= os.copyfile, dry=True, *args):
    """Perform data delivery from a casava-structured directory to a
    project inbox at UPPMAX. 

    :param project_id: project identifier
    :param flowcell_id: flowcell identifier
    :param dry: dry run
    
    :returns: None
    """
    LOG.info("Initiating data delivery for project {}".format(project_id))


