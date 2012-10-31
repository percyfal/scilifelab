"""bcbio run module"""
import os
import re
import yaml

from scilifelab.utils.misc import filtered_walk, query_yes_no
from scilifelab.utils.dry import dry_write, dry_backup, dry_unlink, dry_rmdir
from scilifelab.log import minimal_logger

LOG = minimal_logger(__name__)

# The analysis script for running the pipeline in parallell mode (on one node)  
PARALLELL_ANALYSIS_SCRIPT="automated_initial_analysis.py"
# The analysis script for running the pipeline in distributed mode (across multiple nodes/cores)
DISTRIBUTED_ANALYSIS_SCRIPT="distributed_nextgen_pipeline.py"
# If True, will sanitize the run_info.yaml configuration file when running non-CASAVA analysis
PROCESS_YAML = True
# If True, will assign the distributed master process and workers to a separate RabbitMQ queue for each flowcell 
FC_SPECIFIC_AMPQ = True

def _sample_status(x):
    """Find the status of a sample.
    
    Look for output files: currently only look for project-summary.csv"""
    if os.path.exists(os.path.join(os.path.dirname(x), "project-summary.csv")):
        return "PASS"
    else:
        return "FAIL"

def find_samples(path, sample=None, pattern = "-bcbb-config.yaml$", only_failed=False, **kw):
    """Find bcbb config files in a path.

    :param path: path to search in
    :param sample: a specific sample, or a file consisting of -bcbb-config.yaml files
    :param pattern: pattern to search for

    :returns: list of file names
    """
    flist = []
    if sample:
        if os.path.exists(sample):
            with open(sample) as fh:
                flist = [x.rstrip() for x in fh.readlines()]
        else:
            pattern = "{}{}".format(sample, pattern)
    def bcbb_yaml_filter(f):
        return re.search(pattern, f) != None
    if not flist:
        flist = filtered_walk(path, bcbb_yaml_filter)
    if only_failed:
        status = {x:_sample_status(x) for x in flist}
        flist = [x for x in flist if _sample_status(x)=="FAIL"]
    if len(flist) == 0 and sample:
        LOG.info("No such sample {}".format(sample))
    return [os.path.abspath(f) for f in flist]

def setup_sample(f, analysis_type, google_report=False, no_only_run=False, amplicon=False, genome_build="hg19", **kw):
    """Setup config files, making backups and writing new files

    :param path: root path in which to search for samples
    :param dry_run: dry run flag
    """
    with open(f) as fh:
        config = yaml.load(fh)
    ## Check for correctly formatted config
    if not config.get("details", None):
        LOG.warn("Couldn't find 'details' section in config file: aborting setup!")
        return
    ## Save file to backup if backup doesn't exist
    f_bak = f.replace("-bcbb-config.yaml", "-bcbb-config.yaml.bak")
    if not os.path.exists(f_bak):
        LOG.info("Making backup of {} in {}".format(f, f_bak))
        dry_backup(os.path.abspath(f), dry_run=kw['dry_run'])

    ## Save command file to backup if it doesn't exist
    cmdf = f.replace("-bcbb-config.yaml", "-bcbb-command.txt")
    cmdf_bak = cmdf.replace("-bcbb-command.txt", "-bcbb-command.txt.bak")
    if not os.path.exists(cmdf_bak):
        LOG.info("Making backup of {} in {}".format(cmdf, cmdf_bak))
        dry_backup(os.path.abspath(cmdf), dry_run=kw['dry_run'])

    ## Save post_process file to backup if it doesn't exist
    ppf = f.replace("-bcbb-config.yaml", "-post_process.yaml")
    ppf_bak = ppf.replace("-post_process.yaml", "-post_process.yaml.bak")
    if not os.path.exists(ppf_bak):
        LOG.info("Making backup of {} in {}".format(ppf, ppf_bak))
        dry_backup(ppf, dry_run=kw['dry_run'])

    ## FIXME: write cleaner way of updating config
    nsamples = len(config["details"])
    for i in range(0, nsamples):
        if analysis_type and config["details"][i]["multiplex"][0]["analysis"] != analysis_type:
            LOG.info("Setting analysis_type to {} for sample {}".format(analysis_type, config["details"][i]["multiplex"][0]["name"]))
        
            config["details"][i]["multiplex"][0]["analysis"] = analysis_type
            config["details"][i]["analysis"] = analysis_type
        if config["details"][i]["genome_build"] == 'unknown' or config["details"][i]["multiplex"][0]["genome_build"] != genome_build:
            LOG.info("Setting genome_build to {}".format(genome_build))
            config["details"][i]["genome_build"] = genome_build
            config["details"][i]["multiplex"][0]["genome_build"] = genome_build
        ## Check if files exist: if they don't, then change the suffix
        config["details"][i]["multiplex"][0]["files"].sort()
        seqfiles = config["details"][i]["multiplex"][0]["files"]
        if not os.path.exists(seqfiles[0]):
            (_, ext) = os.path.splitext(seqfiles[0])
            LOG.warn("Couldn't find {} file; will look for {} files".format(os.path.abspath(seqfiles[0]), ext))
            if ext == ".gz":
                config["details"][i]["multiplex"][0]["files"] = [x.replace(".gz", "") for x in seqfiles]
            else:
                config["details"][i]["multiplex"][0]["files"] = ["{}.gz".format(x) for x in seqfiles]

    ## Remove config file and rewrite
    dry_unlink(f, kw['dry_run'])
    dry_write(f, yaml.dump(config), dry_run=kw['dry_run'])

    ## Setup post process
    ppfile = f.replace("-bcbb-config.yaml", "-post_process.yaml")
    with open(ppfile) as fh:
        pp = yaml.load(fh)

    ## Need to set working directory to path of bcbb-config.yaml file
    if pp.get('distributed', {}).get('platform_args', None):
        platform_args = pp['distributed']['platform_args'].split()
        if "-D" in platform_args:
            platform_args[platform_args.index("-D")+1] = os.path.dirname(f)
        elif "--workdir" in platform_args:
            platform_args[platform_args.index("--workdir")+1] = os.path.dirname(f)
        pp['distributed']['platform_args'] = " ".join(platform_args)
    if kw['baits']:
        pp['custom_algorithms'][analysis_type]['hybrid_bait'] = kw['baits']
    if kw['targets']:
        pp['custom_algorithms'][analysis_type]['hybrid_target'] = kw['targets']
    if amplicon:
        LOG.info("setting amplicon analysis")
        pp['algorithm']['mark_duplicates'] = False
        pp['custom_algorithms'][analysis_type]['mark_duplicates'] = False
    if kw['distributed']:
        LOG.info("setting distributed execution")
        pp['algorithm']['num_cores'] = 'messaging'
    else:
        LOG.info("setting parallell execution")
        pp['algorithm']['num_cores'] = kw['num_cores']
    dry_unlink(ppfile, dry_run=kw['dry_run'])
    dry_write(ppfile, yaml.safe_dump(pp, default_flow_style=False, allow_unicode=True, width=1000), dry_run=kw['dry_run'])


def remove_files(f, **kw):
    ## Remove old files if requested
    keep_files = ["-post_process.yaml$", "-post_process.yaml.bak$", "-bcbb-config.yaml$", "-bcbb-config.yaml.bak$",  "-bcbb-command.txt$", "-bcbb-command.txt.bak$", "_[0-9]+.fastq$", "_[0-9]+.fastq.gz$",
                  "^[0-9][0-9]_.*.txt$", "JOBID", "PID"]
    pattern = "|".join(keep_files)
    def remove_filter_fn(f):
        return re.search(pattern, f) == None

    workdir = os.path.dirname(f)
    remove_files = filtered_walk(workdir, remove_filter_fn)
    remove_dirs = filtered_walk(workdir, remove_filter_fn, get_dirs=True)
    if len(remove_files) == 0:
        pass
    if len(remove_files) > 0 and query_yes_no("Going to remove {} files and {} directories... Are you sure you want to continue?".format(len(remove_files), len(remove_dirs)), force=kw['force']):
        [dry_unlink(x, dry_run=kw['dry_run']) for x in remove_files]
        ## Sort directories by length so we don't accidentally try to remove a non-empty dir
        [dry_rmdir(x, dry_run=kw['dry_run']) for x in sorted(remove_dirs, key=lambda x: len(x), reverse=True)]

def run_bcbb_command(run_info, post_process=None, **kw):
    """Setup bcbb command to run
    
    :param run_info: run info file 
    :param post_process: post process file
    :param kw: keyword arguments
    
    :returns: command line to run
    """
    if not post_process:
        post_process = run_info.replace("-bcbb-config.yaml", "-post_process.yaml")
    with open(post_process, "r") as fh:
        config = yaml.load(fh)
    if str(config["algorithm"]["num_cores"]) == "messaging":
        analysis_script = DISTRIBUTED_ANALYSIS_SCRIPT
    else:
        analysis_script = PARALLELL_ANALYSIS_SCRIPT
    platform_args = config["distributed"]["platform_args"].split()
    cl = [analysis_script, post_process, os.path.dirname(run_info), run_info]
    return (cl, platform_args)
