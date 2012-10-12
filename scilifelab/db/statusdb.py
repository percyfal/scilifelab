"""Database backend for connecting to statusdb"""
import re
from itertools import izip
from scilifelab.db import Couch

def match_project_name_to_barcode_name(project_sample_name, sample_run_name):
    """Name mapping from project summary sample id to run info sample id"""
    if not project_sample_name.startswith("P"):
        sid = re.search("(\d+)_?([A-Z])?_",sample_run_name)
        if str(sid.group(1)) == str(project_sample_name):
            return True
        m = re.search("(_index[0-9]+)", sample_run_name)
        if not m:
            index = ""
        else:
            index = m.group(1)
        sid = re.search("([A-Za-z0-9\_]+)(\_index[0-9]+)?", sample_run_name.replace(index, ""))
        if str(sid.group(1)) == str(project_sample_name):
            return True
        else:
            return False
    if str(sample_run_name).startswith(str(project_sample_name)):
        return True
    elif str(sample_run_name).startswith(str(project_sample_name).rstrip("F")):
        return True
    elif str(sample_run_name).startswith(str(project_sample_name).rstrip("B")):
        return True
    ## Add cases here
    return False

def sample_map_fn_id(sample_run_name, prj_sample):
    if 'sample_run_metrics' in prj_sample.keys():
        return prj_sample.get('sample_run_metrics').get(sample_run_name, None)
    else:
        return None

def _prune_ps_map(ps_map):
    """Only use srm_ids that end with [ACGT]+ or "NoIndex"""
    if not ps_map:
        return None
    ret = {}
    for k, v in ps_map.items():
        if re.search("_[ACGT]+$|_NoIndex$", k):
            ret[k] = v
    return ret

class SampleRunMetricsConnection(Couch):
    ## FIXME: set time limits on which entries to include?
    def __init__(self, **kwargs):
        super(SampleRunMetricsConnection, self).__init__(**kwargs)
        self.db = self.con["samples"]
        self.name_view = {k.key:k.id for k in self.db.view("names/name", reduce=False)}
        self.name_fc_view = {k.key:k for k in self.db.view("names/name_fc", reduce=False)}
        self.name_proj_view = {k.key:k for k in self.db.view("names/name_proj", reduce=False)}
        self.name_fc_proj_view = {k.key:k for k in self.db.view("names/name_fc_proj", reduce=False)}

    def get_entry(self, name, field=None):
        """Retrieve entry from db for a given name, subset to field if
        that value is passed.

        :param name: unique name
        :param field: database field

        :returns: value if entry exists, None otherwise
        """
        self.log.debug("retrieving entry in field '{}' for name '{}'".format(field, name))
        if self.name_view.get(name, None) is None:
            self.log.warn("no field '{}' for name '{}'".format(field, name))
            return None
        if field:
            return self.db.get(self.name_view.get(name))[field]
        else:
            return self.db.get(self.name_view.get(name))

    def get_sample_ids(self, fc_id=None, sample_prj=None):
        """Retrieve sample ids subset by fc_id and/or sample_prj

        :param fc_id: flowcell id
        :param sample_prj: sample project name

        :returns sample_ids: list of couchdb sample ids
        """
        self.log.debug("retrieving sample ids subset by flowcell '{}' and sample_prj '{}'".format(fc_id, sample_prj))
        fc_sample_ids = [self.name_fc_view[k].id for k in self.name_fc_view.keys() if self.name_fc_view[k].value == fc_id] if fc_id else []
        prj_sample_ids = [self.name_proj_view[k].id for k in self.name_proj_view.keys() if self.name_proj_view[k].value == sample_prj] if sample_prj else []
        sample_ids = list(set(fc_sample_ids + prj_sample_ids))
        return sample_ids

    def get_samples(self, fc_id=None, sample_prj=None):
        """Retrieve samples subset by fc_id and/or sample_prj

        :param fc_id: flowcell id
        :param sample_prj: sample project name

        :returns samples: list of samples
        """
        self.log.debug("retrieving samples subset by flowcell '{}' and sample_prj '{}'".format(fc_id, sample_prj))
        sample_ids = self.get_sample_ids(fc_id, sample_prj)
        return [self.db.get(x) for x in sample_ids]
        
    def set_db(self):
        """Make sure we don't change db from samples"""
        pass

    ## FIX ME: operations on sample run metrics objects should be
    ## separated from the connection. Either implement a
    ## sample_run_metrics object (subclassing ViewResults) with this
    ## function or move to utils or similar
    def calc_avg_qv(self, name):
        """Calculate average quality score for a sample based on
        FastQC results.
        
        FastQC reports QV results in the field 'Per sequence quality scores', 
        where the subfields 'Count' and 'Quality' refer to the counts of a given quality value.
        
        :param name: sample name
        
        :returns avg_qv: Average quality value score.
        """
        srm = self.get_entry(name)
        try:
            count = [float(x) for x in srm["fastqc"]["stats"]["Per sequence quality scores"]["Count"]]
            quality = srm["fastqc"]["stats"]["Per sequence quality scores"]["Quality"]
            return round(sum([x*int(y) for x,y in izip(count, quality)])/sum(count), 1)
        except:
            self.log.warn("Calculation of average quality failed for sample {}, id {}".format(srm["name"], srm["_id"]))
            return None

    def get_qc_data(self, sample_prj, fc_id=None):
        """Get qc data for a project, possibly subset by project_id"""
        p_con = ProjectSummaryConnection(url=self.url, username=self.user, password=self.pw)
        project = p_con.get_entry(sample_prj)
        application = project.get("application", None) if project else None
        sample_ids = [self.name_proj_view[k].id for k in self.name_proj_view.keys() if self.name_proj_view[k].value == sample_prj]
        if fc_id:
            fc_sample_ids = [self.name_fc_view[k].id for k in self.name_fc_view.keys() if self.name_fc_view[k].value == fc_id]
            sample_ids = list(set(sample_ids).intersection(set(fc_sample_ids)))
        samples = [self.db.get(x) for x in sample_ids]
        qcdata = {}
        for s in samples:
            qcdata[s["name"]]={"sample":s.get("barcode_name", None),
                               "project":s.get("sample_prj", None),
                               "lane":s.get("lane", None),
                               "flowcell":s.get("flowcell", None),
                               "date":s.get("date", None),
                               "application":application,
                               "TOTAL_READS":s.get("picard_metrics", {}).get("AL_PAIR", {}).get("TOTAL_READS", -1),
                               "PERCENT_DUPLICATION":s.get("picard_metrics", {}).get("DUP_metrics", {}).get("PERCENT_DUPLICATION", "-1.0"),
                               "MEAN_INSERT_SIZE":s.get("picard_metrics", {}).get("INS_metrics", {}).get("MEAN_INSERT_SIZE", "-1.0"),
                               "GENOME_SIZE":s.get("picard_metrics", {}).get("HS_metrics", {}).get("GENOME_SIZE", -1),
                               "FOLD_ENRICHMENT":s.get("picard_metrics", {}).get("HS_metrics", {}).get("FOLD_ENRICHMENT", "-1.0").replace(",", "."),
                               "PCT_USABLE_BASES_ON_TARGET":s.get("picard_metrics", {}).get("HS_metrics", {}).get("PCT_USABLE_BASES_ON_TARGET", "-1.0"),
                               "PCT_TARGET_BASES_10X":s.get("picard_metrics", {}).get("HS_metrics", {}).get("PCT_TARGET_BASES_10X", "-1.0"),
                               "PCT_PF_READS_ALIGNED":s.get("picard_metrics", {}).get("AL_PAIR", {}).get("PCT_PF_READS_ALIGNED", "-1.0"),
                               }
            target_territory = s.get("picard_metrics", {}).get("HS_metrics", {}).get("TARGET_TERRITORY", -1)
            pct_labels = ["PERCENT_DUPLICATION", "PCT_USABLE_BASES_ON_TARGET", "PCT_TARGET_BASES_10X",
                          "PCT_PF_READS_ALIGNED"]
            for l in pct_labels:
                if qcdata[s["name"]][l]:
                    qcdata[s["name"]][l] = float(qcdata[s["name"]][l].replace(",", ".")) * 100
            if qcdata[s["name"]]["FOLD_ENRICHMENT"] and qcdata[s["name"]]["GENOME_SIZE"] and target_territory:
                qcdata[s["name"]]["PERCENT_ON_TARGET"] = float(float(qcdata[s["name"]]["FOLD_ENRICHMENT"].replace(",", "."))/(float(qcdata[s["name"]]["GENOME_SIZE"]) / float(target_territory))) * 100
        return qcdata


class FlowcellRunMetricsConnection(Couch):
    def __init__(self, **kwargs):
        super(FlowcellRunMetricsConnection, self).__init__(**kwargs)
        if not self.con:
            return
        self.db = self.con["flowcells"]
        self.name_view = {k.key:k.id for k in self.db.view("names/name", reduce=False)}

    def set_db(self):
        """Make sure we don't change db from flowcells"""
        pass

    def get_entry(self, name, field=None):
        """Retrieve entry from db for a given name, subset to field if
        that value is passed.
        """
        self.log.debug("retrieving field entry in field '{}' for name '{}'".format(field, name))
        if self.name_view.get(name, None) is None:
            self.log.warn("no field '{}' for name '{}'".format(field, name))
            return None
        if field:
            return self.db.get(self.name_view.get(name))[field]
        else:
            return self.db.get(self.name_view.get(name))

    def get_phix_error_rate(self, name, lane, avg=True):
        """Get phix error rate"""
        fc = self.get_entry(name)
        phix_r1 = float(fc['illumina']['Summary']['read1'][lane]['ErrRatePhiX']) 
        phix_r2 = float(fc['illumina']['Summary']['read3'][lane]['ErrRatePhiX'])
        if avg:
            return (phix_r1 + phix_r2)/2
        else:
            return (phix_r1, phix_r2)/2

class ProjectSummaryConnection(Couch):
    def __init__(self, **kwargs):
        super(ProjectSummaryConnection, self).__init__(**kwargs)
        if not self.con:
            return
        self.db = self.con["projects"]
        self.name_view = {k.key:k.id for k in self.db.view("project/project_id", reduce=False)}

    def get_entry(self, name, field=None):
        """Retrieve entry from db for a given name, subset to field if
        that value is passed.

        :param name: unique name
        :param field: database field

        :returns: value if entry exists, None otherwise
        """
        self.log.debug("retrieving field entry in field '{}' for name '{}'".format(field, name))
        if self.name_view.get(name, None) is None:
            self.log.warn("no field '{}' for name '{}'".format(field, name))
            return None
        if field:
            return self.db.get(self.name_view.get(name))[field]
        else:
            return self.db.get(self.name_view.get(name))

    def set_db(self):
        """Make sure we don't change db from projects"""
        pass

    def map_srm_to_name(self, project_id, include_all=True, **args):
        """Map sample run metrics names to project sample names for a
        project, possibly subset by flowcell id.

        :param project_id: project id
        :param **kw: keyword arguments to be passed to map_name_to_srm
        """
        samples = self.map_name_to_srm(project_id, **args)
        srm_to_name = {}
        for k, v in samples.items():
            if not v:
                if not include_all:
                    continue
                srm_to_name.update({"NOSRM_{}".format(k):{"sample":k, "id":None}})
            else:
                srm_to_name.update({x:{"sample":k,"id":y} for x,y in v.items()})
        return srm_to_name

    def map_name_to_srm(self, project_id, fc_id=None, use_ps_map=True, use_bc_map=False,  check_consistency=False):
        """Map project sample names to sample run metrics names for a
        project, possibly subset by flowcell id.

        :param project_id: project id
        :param fc_id: flowcell id
        :param use_ps_map: use and give precedence to the project summary mapping to sample run metrics (default True)
        :param use_bc_map: use and give precedence to the barcode match mapping to sample run metrics (default False)
        :param check_consistency: use both mappings and check consistency (default False)
        """
        project = self.get_entry(project_id)
        if not project:
            return None
        project_samples = project.get('samples', None)
        if project_samples is None:
            return None
        sample_map = {}
        s_con = SampleRunMetricsConnection(username=self.user, password=self.pw, url=self.url)
        srm_samples = s_con.get_samples(fc_id=fc_id, sample_prj=project_id)
        for k, v in project_samples.items():
            sample_map[k] = None
            if check_consistency:
                use_ps_map = True
                use_bc_map = True
            if use_ps_map:
                ps_map = v.get('sample_run_metrics', None)
                sample_map[k] = _prune_ps_map(ps_map)
            if use_bc_map or not sample_map[k]:
                if not sample_map[k]: self.log.info("Using barcode map since no information in project summary for sample '{}'".format(k))
                bc_map = {s["name"]:s["_id"] for s in srm_samples if match_project_name_to_barcode_name(k, s.get("barcode_name", None))}
                sample_map[k] = bc_map
            if check_consistency:
                if ps_map == bc_map:
                    self.log.debug("Sample {} has consistent mappings!".format(k))
                else:
                    self.log.warn("Sample {} has inconsistent mappings: ps_map {} vs barcode_match {}".format(k, ps_map, bc_map))
        return sample_map
        
    def get_ordered_amount(self, project_id, rounded=True, dec=1):
        """Get (rounded) ordered amount of reads in millions. 

        :param project_id: project id
        :param rounded: <boolean>
        :param dec: <integer>, number of decimal places

        :returns: ordered amount of reads if present, None otherwise
        """
        amount = self.get_entry(project_id, 'min_m_reads_per_sample_ordered')
        self.log.debug("got amount {}".format(amount))
        if not amount:
            return None
        else:
            return round(amount, dec)
