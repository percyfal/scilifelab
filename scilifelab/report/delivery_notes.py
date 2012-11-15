"""Module delivery_notes - code for generating delivery reports and notes"""
import os
import re
import itertools
import ast
import json
from cStringIO import StringIO
from collections import Counter
from scilifelab.db.statusdb import SampleRunMetricsConnection, ProjectSummaryConnection, FlowcellRunMetricsConnection, calc_avg_qv
from scilifelab.report import sequencing_success
from scilifelab.report.rl import make_note, concatenate_notes, sample_note_paragraphs, sample_note_headers, project_note_paragraphs, project_note_headers, make_sample_table
import scilifelab.log

LOG = scilifelab.log.minimal_logger(__name__)


def sample_status_note(project_id=None, flowcell_id=None, user=None, password=None, url=None,
                       use_ps_map=True, use_bc_map=False, check_consistency=False, 
                       ordered_million_reads=None, uppnex_id=None, customer_reference=None,
                       no_qcinfo=True, **kw):
    """Make a sample status note. Used keywords:

    :param project_id: project id
    :param flowcell_id: flowcell id
    :param user: db user name
    :param password: db password
    :param url: db url
    :param use_ps_map: use project summary mapping
    :param use_bc_map: use project to barcode name mapping
    :param check_consistency: check consistency between mappings
    :param ordered_million_reads: number of ordered reads in millions
    :param uppnex_id: the uppnex id
    :param customer_reference: customer project name
    """
    ## Cutoffs
    cutoffs = {
        "phix_err_cutoff" : 2.0,
        "qv_cutoff" : 30,
        }
    
    ## parameters
    parameters = {
        "project_name" : None,
        "start_date" : None,
        "FC_id" : None,
        "scilifelab_name" : None,
        "rounded_read_count" : None,
        "phix_error_rate" : None,
        "avg_quality_score" : None,
        "success" : None,
        }
        ## key mapping from sample_run_metrics to parameter keys
    srm_to_parameter = {"project_name":"sample_prj", "FC_id":"flowcell", 
                        "scilifelab_name":"barcode_name", "start_date":"date", "rounded_read_count":"bc_count"}
    
    LOG.debug("got parameters {}".format(parameters))
    output_data = {'stdout':StringIO(), 'stderr':StringIO()}
    output_data["stdout"].write("\nQuality stats\n")
    output_data["stdout"].write("************************\n")
    output_data["stdout"].write("PhiX error cutoff: > {:3}\n".format(cutoffs['phix_err_cutoff']))
    output_data["stdout"].write("QV cutoff        : < {:3}\n".format(cutoffs['qv_cutoff']))
    output_data["stdout"].write("************************\n\n")
    output_data["stdout"].write("{:>18}\t{:>12}\t{:>12}\t{:>12}\t{:>12}\n".format("Scilifelab ID", "PhiXError", "ErrorStatus", "AvgQV", "QVStatus"))
    output_data["stdout"].write("{:>18}\t{:>12}\t{:>12}\t{:>12}\t{:>12}\n".format("=============", "=========", "===========", "=====", "========"))
    ## Connect and run
    s_con = SampleRunMetricsConnection(username=user, password=password, url=url)
    fc_con = FlowcellRunMetricsConnection(username=user, password=password, url=url)
    p_con = ProjectSummaryConnection(username=user, password=password, url=url)
    paragraphs = sample_note_paragraphs()
    headers = sample_note_headers()
    project = p_con.get_entry(project_id)
    notes = []
    if not project:
        LOG.warn("No such project '{}'".format(project_id))
        return output_data
    samples = s_con.get_samples(sample_prj=project_id, fc_id=flowcell_id)
    if len(samples) == 0:
        LOG.warn("No samples for project '{}', flowcell '{}'. Maybe there are no sample run metrics in statusdb?".format(project_id, flowcell_id))
        return output_data
    sample_count = Counter([x.get("barcode_name") for x in samples])
    for s in samples:
        s_param = {}
        LOG.debug("working on sample '{}', sample run metrics name '{}', id '{}'".format(s.get("barcode_name", None), s.get("name", None), s.get("_id", None)))
        s_param.update(parameters)
        s_param.update({key:s[srm_to_parameter[key]] for key in srm_to_parameter.keys()})
        fc = "{}_{}".format(s.get("date"), s.get("flowcell"))
        s_param["phix_error_rate"] = fc_con.get_phix_error_rate(str(fc), s["lane"])
        s_param['avg_quality_score'] = calc_avg_qv(s)
        if not s_param['avg_quality_score']:
            LOG.warn("Calculation of average quality failed for sample {}, id {}".format(s.get("name"), s.get("_id")))
        err_stat = "OK"
        qv_stat = "OK"
        if s_param["phix_error_rate"] > cutoffs["phix_err_cutoff"]:
            err_stat = "HIGH"
        if s_param["avg_quality_score"] < cutoffs["qv_cutoff"]:
            qv_stat = "LOW"
        output_data["stdout"].write("{:>18}\t{:>12}\t{:>12}\t{:>12}\t{:>12}\n".format(s["barcode_name"], s_param["phix_error_rate"], err_stat, s_param["avg_quality_score"], qv_stat))
        s_param['rounded_read_count'] = round(float(s_param['rounded_read_count'])/1e6,1) if s_param['rounded_read_count'] else None
        s_param['ordered_amount'] = s_param.get('ordered_amount', p_con.get_ordered_amount(project_id))
        s_param['customer_reference'] = s_param.get('customer_reference', project.get('customer_reference'))
        s_param['uppnex_project_id'] = s_param.get('uppnex_project_id', project.get('uppnex_id'))
        if ordered_million_reads:
            s_param["ordered_amount"] = ordered_million_reads
        if uppnex_id:
            s_param["uppnex_project_id"] = uppnex_id
        if customer_reference:
            s_param["customer_reference"] = customer_reference
        ## FIX ME: This is where we need a key in SampleRunMetrics that provides a mapping to a project sample name
        project_sample = p_con.get_project_sample(project_id, s["barcode_name"])
        if project_sample:
            if "library_prep" in project_sample.keys():
                project_sample_d = {x:y for d in [v["sample_run_metrics"] for k,v in project_sample["library_prep"].iteritems()] for x,y in d.iteritems()}
            else:
                project_sample_d = {x:y for x,y in project_sample["sample_run_metrics"].iteritems()}
            if s["name"] not in project_sample_d.keys():
                LOG.warn("'{}' not found in project sample run metrics for project".format(s["name"]) )
            else:
                if s["_id"] == project_sample_d[s["name"]]:
                    LOG.debug("project sample run metrics mapping found: '{}' : '{}'".format(s["name"], project_sample_d[s["name"]]))
                else:
                    LOG.warn("inconsistent mapping for '{}': '{}' != '{}' (project summary id)".format(s["name"], s["_id"], project_sample_d[s["name"]]))
            s_param['customer_name'] = project_sample.get("customer_name", None)
        else:
            s_param['customer_name'] = None
            LOG.warn("No project sample name found for sample run name '{}'".format(s["barcode_name"]))
        s_param['success'] = sequencing_success(s_param, cutoffs)
        s_param.update({k:"N/A" for k in s_param.keys() if s_param[k] is None or s_param[k] ==  ""})
        if sample_count[s.get("barcode_name")] > 1:
            outfile = "{}_{}_{}_{}.pdf".format(s["barcode_name"], s["date"], s["flowcell"], s["lane"])
        else:
            outfile = "{}_{}_{}.pdf".format(s["barcode_name"], s["date"], s["flowcell"])
        notes.append(make_note(outfile, headers, paragraphs, **s_param))
    concatenate_notes(notes, "{}_{}_{}_sample_summary.pdf".format(project_id, s.get("date", None), s.get("flowcell", None)))
    return output_data

def project_status_note(project_id=None, user=None, password=None, url=None,
                        use_ps_map=True, use_bc_map=False, check_consistency=False,
                        ordered_million_reads=None, uppnex_id=None, customer_reference=None,
                        exclude_sample_ids={}, project_alias=None, sample_aliases={}, **kw):
    """Make a project status note. Used keywords:

    :param project_id: project id
    :param user: db user name
    :param password: db password
    :param url: db url
    :param use_ps_map: use project summary mapping
    :param use_bc_map: use project to barcode name mapping
    :param check_consistency: check consistency between mappings
    :param ordered_million_reads: number of ordered reads in millions
    :param uppnex_id: the uppnex id
    :param customer_reference: customer project name
    :param exclude_sample_ids: exclude some sample ids from project note
    :param project_alias: project alias name
    :param sample_aliases: sample alias names
    """
    ## parameters
    parameters = {
        "project_name" : project_id,
        "finished" : "Not finished, or cannot yet assess if finished.",
        }
    ## mapping project_summary to parameter keys
    ps_to_parameter = {"scilife_name":"scilife_name", "customer_name":"customer_name", "project_name":"project_id"}
    ## mapping project sample to table
    table_keys = ['ScilifeID', 'CustomerID', 'BarcodeSeq', 'MSequenced', 'MOrdered', 'Status']
    prjs_to_table = {'ScilifeID':'scilife_name', 'CustomerID':'customer_name', 'MSequenced':'m_reads_sequenced'}#, 'MOrdered':'min_m_reads_per_sample_ordered', 'Status':'status'}
        
    ## Connect and run
    s_con = SampleRunMetricsConnection(username=user, password=password, url=url)
    fc_con = FlowcellRunMetricsConnection(username=user, password=password, url=url)
    p_con = ProjectSummaryConnection(username=user, password=password, url=url)
    paragraphs = project_note_paragraphs()
    headers = project_note_headers()
    param = parameters
    if not project_alias:
        project = p_con.get_entry(project_id)
    else:
        project = p_con.get_entry(project_alias)

    if sample_aliases:
        if os.path.exists(sample_aliases):
            with open(sample_aliases) as fh:
                sample_aliases = json.load(fh)
        else:
            sample_aliases = ast.literal_eval(sample_aliases)
    if not project:
        LOG.warn("No such project '{}'".format(project_id))
        return
    LOG.debug("Working on project '{}'.".format(project_id))
    slist = s_con.get_samples(sample_prj=project_id)
    samples = {}
    for s in slist:
        prj_sample = p_con.get_project_sample(project_id, s["barcode_name"])
        if prj_sample:
            s_d = {s["name"] : {'sample':prj_sample["scilife_name"], 'id':s["_id"]}}
            samples.update(s_d)
        else:
            if s["barcode_name"] in sample_aliases:
                s_d = {sample_aliases[s["barcode_name"]] : {'sample':sample_aliases[s["barcode_name"]], 'id':s["_id"]}}
                samples.update(s_d)
            else:
                s_d = {s["name"]:{'sample':s["name"], 'id':s["_id"], 'barcode_name':s["barcode_name"]}}
                LOG.warn("No mapping found for sample run:\n  '{}'".format(s_d))
    ## Convert to mapping from desired sample name to list of aliases
    ## Less important for the moment; one solution is to update the
    ## Google docs summary table to use the P names
    sample_list = project['samples']
    param.update({key:project.get(ps_to_parameter[key], None) for key in ps_to_parameter.keys()})
    param["ordered_amount"] = param.get("ordered_amount", p_con.get_ordered_amount(project_id))
    param['customer_reference'] = param.get('customer_reference', project.get('customer_reference'))
    param['uppnex_project_id'] = param.get('uppnex_project_id', project.get('uppnex_id'))
    if ordered_million_reads:
        param["ordered_amount"] = ordered_million_reads
    if uppnex_id:
        param["uppnex_project_id"] = uppnex_id
    if customer_reference:
        param["customer_reference"] = customer_reference
    if not param["ordered_amount"]:
        param["ordered_amount"] = ordered_million_reads
    if exclude_sample_ids:
        if os.path.exists(exclude_sample_ids):
            with open(exclude_sample_ids) as fh:
                exclude_sample_ids = json.load(fh)
        else:
            exclude_sample_ids = ast.literal_eval(exclude_sample_ids)
    ## Start collecting the data
    sample_table = []
    all_passed = True
    LOG.debug("Looping through sample map that maps project sample names to sample run metrics ids")
    for k,v in samples.items():
        LOG.debug("project sample '{}' maps to '{}'".format(k, v))
        if re.search("Unexpected", k):
            continue
        barcode_seq = s_con.get_entry(k, "sequence")
        if exclude_sample_ids and v['sample'] in exclude_sample_ids.keys():
            if exclude_sample_ids[v['sample']]:
                if barcode_seq in exclude_sample_ids[v['sample']]:
                    LOG.info("excluding sample '{}' with barcode '{}' from project report".format(v['sample'], barcode_seq))
                    continue
                else:
                    LOG.info("keeping sample '{}' with barcode '{}' in sequence report".format(v['sample'], barcode_seq))
            else:
                LOG.info("excluding sample '{}' from project report".format(v['sample']))
                continue
        project_sample = sample_list[v['sample']]
        vals = {x:project_sample.get(prjs_to_table[x], None) for x in prjs_to_table.keys()}
        ## Set status
        vals['Status'] = project_sample.get("status", "N/A")
        vals['MOrdered'] = param["ordered_amount"]
        vals['BarcodeSeq'] = barcode_seq
        vals.update({k:"N/A" for k in vals.keys() if vals[k] is None or vals[k] == ""})
        if vals['Status']=="N/A" or vals['Status']=="NP": all_passed = False
        sample_table.append([vals[k] for k in table_keys])
    if all_passed: param["finished"] = 'Project finished.'
    sample_table.sort()
    sample_table = list(sample_table for sample_table,_ in itertools.groupby(sample_table))
    sample_table.insert(0, ['ScilifeID', 'CustomerID', 'BarcodeSeq', 'MSequenced', 'MOrdered', 'Status'])
    paragraphs["Samples"]["tpl"] = make_sample_table(sample_table)
    make_note("{}_project_summary.pdf".format(project_id), headers, paragraphs, **param)

