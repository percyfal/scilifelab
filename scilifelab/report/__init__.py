"""Reporting utilities module"""
import os
import sys
from mako.template import Template
from collections import OrderedDict

from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.rl_config import defaultPageSize
import scilifelab.log

LOG = scilifelab.log.minimal_logger(__name__)

FILEPATH=os.path.dirname(os.path.realpath(__file__))
             
def sequencing_success(parameters, cutoffs):
    """Set sequencing success for a sample. It is assumed that ordered
    comparisons are done in units "millions".

    :param parameters: Collected parameters for a sample
    :param cutoffs: Cutoff values for key QC data

    :returns: string
    """
    success_message = ''
    LOG.debug("Received parameters {} and cutoffs {}".format(parameters, cutoffs))
    if not parameters['ordered_amount']:
        LOG.warn("No ordered amount provided. Please check the project summary and make sure min_m_reads_per_sample_ordered is a number.")
        return "Could not assess success or failure of run."
    if parameters['phix_error_rate'] == 0:
        LOG.warn("Phix error rate is zero: this usually indicates some problem with the flowcell! Please check with the lab")
        return "Could not assess success or failure of run."
    if parameters['phix_error_rate'] == -1:
        LOG.warn("No phix error rate available! Please supply manually with option '--phix'")
        return "Could not assess success or failure of run."
    try:
        if float(parameters['phix_error_rate']) < cutoffs['phix_err_cutoff'] and float(parameters['rounded_read_count']) > float(parameters['ordered_amount']):
            success_message += "Successful run."
        else:
            if float(parameters['phix_error_rate']) > cutoffs['phix_err_cutoff']: success_message += "High average error rate."
            if float(parameters['rounded_read_count']) < float(parameters['ordered_amount']): success_message += "The yield may be lower than expected."
    except ValueError as e:
        LOG.warn(e)
        success_message = "Could not assess success or failure of run."
    return success_message

def set_status(parameters):
    """Set status for a sample.

    :param parameters: Collected parameters for a  sample
    
    :returns: string
    """
    status = "N/A"
    if 'rounded_read_count' in parameters.keys() and 'ordered_amount' in parameters.keys():
        if parameters['rounded_read_count'] >= parameters['ordered_amount']:
            status = "P"
    else:
        status = None
    return status

