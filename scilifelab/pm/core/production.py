"""Pm production module"""

import sys
import os
import re
import subprocess
from cement.core import controller
from scilifelab.pm.core.controller import AbstractExtendedBaseController
from scilifelab.utils.misc import query_yes_no, filtered_walk
from scilifelab.bcbio.run import find_samples, setup_sample, remove_files, run_bcbb_command
from scilifelab.bcbio.flowcell import Flowcell
from scilifelab.bcbio.status import status_query
from scilifelab.utils.string import strip_extensions

## Main production controller
class ProductionController(AbstractExtendedBaseController):
    """
    Functionality for production management.
    """
    class Meta:
        label = 'production'
        description = 'Manage production'
        arguments = [
            (['project'], dict(help="Project id", nargs="?", default=None)),
            (['-f', '--flowcell'], dict(help="Flowcell id")),
            (['-S', '--sample'], dict(help="project sample id", action="store", default=None, type=str)),
            (['-l', '--lane'], dict(help="Lane id")),
            (['-b', '--barcode_id'], dict(help="Barcode id")),
            (['--from_pre_casava'], dict(help="Use pre-casava directory structure for gathering information", action="store_true", default=False)),
            (['--to_pre_casava'], dict(help="Use pre-casava directory structure for delivery", action="store_true", default=False)),
            (['--transfer_dir'], dict(help="Transfer data to transfer_dir instead of sample_prj dir", action="store", default=None)),
            (['--brief'], dict(help="Output brief information from status queries", action="store_true", default=False)),
            (['--analysis_type'], dict(help="set analysis type in bcbb config file", action="store", default="Align_standard_seqcap", type=str)),
            (['--genome_build'], dict(help="genome build ", action="store", default="hg19", type=str)),
            (['--only_failed'], dict(help="only run on failed samples ", action="store_true", default=False)),
            (['--only_setup'], dict(help="only perform setup", action="store_true", default=False)),
            (['--restart'], dict(help="restart analysis", action="store_true", default=False)),
            (['--distributed'], dict(help="run distributed, changing 'num_cores' in  post_process to 'messaging': calls automated_initial_analysis.py", action="store_true", default=False)),
            (['--num_cores'], dict(help="num_cores value; default 8", action="store", default=8, type=int)),
            (['--amplicon'], dict(help="amplicon-based analyses (e.g. HaloPlex), which means mark_duplicates is set to false", action="store_true", default=False)),
            (['--targets'], dict(help="sequence capture target file", action="store", default=None)),
            (['--baits'], dict(help="sequence capture baits file", action="store", default=None)),
            (['--email'], dict(help="set user email address", action="store", default=None, type=str)),
            ]

    def _process_args(self):
        # Set root path for parent class
        ## FIXME: use abspath?
        self._meta.root_path = self.app.config.get("production", "root")
        assert os.path.exists(self._meta.root_path), "No such directory {}; check your production config".format(self._meta.root_path)
        ## Set path_id for parent class
        if self.pargs.flowcell:
            self._meta.path_id = self.pargs.flowcell
        if self.pargs.project:
            self._meta.path_id = self.pargs.project
        ## Temporary fix for pre-casava directories
        if self.pargs.from_pre_casava:
            self._meta.path_id = self.pargs.flowcell
        ## This is a bug; how will this work when processing casava-folders?!?
        ## I need to set this so as not to upset productioncontrollers process_args
        if self.command == "hs_metrics":
            self._meta.path_id = self.pargs.flowcell if self.pargs.flowcell else self.pargs.project
        super(ProductionController, self)._process_args()

    @controller.expose(hide=True)
    def default(self):
        print self._help_text

    @controller.expose(help="Query the status of flowcells, projects, samples"\
                           " that are organized according to the CASAVA file structure")
    def status_query(self):
        if not self._check_pargs(["project", "flowcell"]):
            return
        status_query(self.app.config.get("archive", "root"), self.app.config.get("production", "root"), self.pargs.flowcell, self.pargs.project, brief=self.pargs.brief)

    def _from_casava_structure(self):
        """Get information from casava structure"""
        if not self._check_pargs(["project"]):
            return
        fc_list = []
        pattern = "-bcbb-config.yaml$"
        def bcbb_yaml_filter(f):
            return re.search(pattern, f) != None
        samples = filtered_walk(os.path.join(self._meta.root_path, self._meta.path_id), bcbb_yaml_filter)
        for s in samples:
            fc = Flowcell(s)
            fc_new = fc.subset("sample_prj", self.pargs.project)
            fc_new.collect_files(os.path.dirname(s))        
            fc_list.append(fc_new)
        return fc_list
            
    def _to_casava_structure(self, fc):
        outdir_pfx = os.path.abspath(os.path.join(self.app.config.get("project", "root"), self.pargs.project.replace(".", "_").lower(), "data"))
        if self.pargs.transfer_dir:
           outdir_pfx = os.path.abspath(os.path.join(self.app.config.get("project", "root"), self.pargs.transfer_dir, "data"))
        for sample in fc:
            key = "{}_{}".format(sample['lane'], sample['sequence'])
            sources = {"files": self._prune_sequence_files(sample['files']), "results":sample['results']}
            outdir = os.path.join(outdir_pfx, sample['name'], fc.fc_id())
            dirs = {"data":os.path.abspath(os.path.join(outdir_pfx, sample['name'], fc.fc_id())),
                    "intermediate":os.path.abspath(os.path.join(outdir_pfx, sample['name'], fc.fc_id()))}
            self._make_output_dirs(dirs)
            fc_new = fc.subset("lane", sample['lane']).subset("name", sample['name'])
            targets = {"files": [src.replace(fc.path, dirs["data"]) for src in sources['files']],
                       "results": [src.replace(fc.path, dirs["intermediate"]) for src in sources['results']]}

            fc_new.lane_files = dict((k, [os.path.join(outdir, os.path.basename(x)) for x in v]) for k,v in fc_new.lane_files.items())
            fc_new.set_entry(key, 'files', targets['files'])
            fc_new.set_entry(key, 'results', targets['results'])
            ## Copy sample files - currently not doing lane files
            self._transfer_files(sources, targets)
            self.app.cmd.write(os.path.join(dirs["data"], "{}-bcbb-pm-config.yaml".format(sample['name'])), fc_new.as_yaml())


    def _to_pre_casava_structure(self, fc):
        dirs = {"data":os.path.abspath(os.path.join(self.app.config.get("project", "root"), self.pargs.project.replace(".", "_").lower(), "data", fc.fc_id())),
                "intermediate":os.path.abspath(os.path.join(self.app.config.get("project", "root"), self.pargs.project.replace(".", "_").lower(), "intermediate", fc.fc_id()))}
        if self.pargs.transfer_dir:
           dirs["data"] = os.path.abspath(os.path.join(self.app.config.get("project", "root"), self.pargs.transfer_dir, "data", fc.fc_id()))
           dirs["intermediate"] = os.path.abspath(os.path.join(self.app.config.get("project", "root"), self.pargs.transfer_dir, "intermediate", fc.fc_id()))
        self._make_output_dirs(dirs)
        fc_new = fc
        for sample in fc:
            key = "{}_{}".format(sample['lane'], sample['sequence'])
            sources = {"files": self._prune_sequence_files(sample['files']), "results":sample['results']}
            targets = {"files": [src.replace(fc.path, dirs["data"]) for src in sources['files']],
                       "results": [src.replace(fc.path, dirs["intermediate"]) for src in sources['results']]}
            fc_new.set_entry(key, 'files', targets['files'])
            fc_new.set_entry(key, 'results', targets['results'])
            ## FIX ME: lane file gathering
            ## fc_new.lane_files = dict((k,[x.replace(indir, outdir) for x in v]) for k,v in fc_new.lane_files.items())
            ## Copy sample files - currently not doing lane files
            self._transfer_files(sources, targets)
        self.app.cmd.write(os.path.join(dirs["data"], "project_run_info.yaml"), fc_new.as_yaml())

    def _from_pre_casava_structure(self):
        if not self._check_pargs(["project", "flowcell"]):
            return
        fc = Flowcell()
        fc.load([os.path.join(x, self.pargs.flowcell) for x in [self.config.get("archive", "root"), self.config.get("production", "root")]])
        indir = os.path.join(self.config.get("production", "root"), self.pargs.flowcell)
        if not fc:
            self.log.warn("No run information available for {}".format(self.pargs.flowcell))
            return
        fc_new = fc.subset("sample_prj", self.pargs.project)
        fc_new.collect_files(indir)        
        return fc_new

    def _prune_sequence_files(self, flist):
        """Sometimes fastq and fastq.gz files are present for the same
        read. Make sure only one file is used, giving precedence to
        the zipped file.
        
        :param flist: list of files
        
        :returns: pruned list of files
        """
        samples = {k[0]:{} for k in [strip_extensions(x, ['.gz']) for x in flist]}
        tmp = {samples[str(k[0])].update({str(k[1]):True}) for k in [strip_extensions(x, ['.gz']) for x in flist]}
        return ["{}.gz".format(k) if v.get('.gz', None) else k for k, v in samples.items()]

    def _make_output_dirs(self, dirs):
        if not os.path.exists(dirs["data"]):
            self.app.cmd.safe_makedir(dirs["data"])
        if not os.path.exists(dirs["intermediate"]):
            self.app.cmd.safe_makedir(dirs["intermediate"])

    def _transfer_files(self, sources, targets):
        for src, tgt in zip(sources['files'] + sources['results'], targets['files'] + targets['results']):
            if not os.path.exists(os.path.dirname(tgt)):
                self.app.cmd.safe_makedir(os.path.dirname(tgt))
            self.app.cmd.transfer_file(src, tgt)

    @controller.expose(help="Transfer data")
    def transfer(self):
        if not self.pargs.from_pre_casava and self.pargs.to_pre_casava:
            self.app.log.warn("not delivering from casava input to pre_casava output")
            return
        ## Collect files depending on input structure
        if self.pargs.from_pre_casava:
            fc = self._from_pre_casava_structure()
        else:
            fc = self._from_casava_structure()
        if not fc:
            return
        ## Organize output file names depending on output structure
        if self.pargs.to_pre_casava:
            self._to_pre_casava_structure(fc)
        else:
            if isinstance(fc, list):
                for f in fc:
                    self._to_casava_structure(f)
            else:
                self._to_casava_structure(fc)


    @controller.expose(help="Run bcbb pipeline")
    def run(self):
        if not self._check_pargs(["project"]):
            return
        flist = find_samples(os.path.abspath(os.path.join(self._meta.root_path, self._meta.path_id)), **vars(self.pargs))
        if len(flist) > 0 and not query_yes_no("Going to start {} jobs... Are you sure you want to continue?".format(len(flist)), force=self.pargs.force):
            return
        orig_dir = os.path.abspath(os.getcwd())
        for run_info in flist:
            os.chdir(os.path.abspath(os.path.dirname(run_info)))
            setup_sample(run_info, **vars(self.pargs))
            os.chdir(orig_dir)
        if self.pargs.only_setup:
            return
        ## Here process files again, removing if requested, and running the pipeline
        for run_info in flist:
            self.app.log.info("Running analysis defined by config file {}".format(run_info))
            os.chdir(os.path.abspath(os.path.dirname(run_info)))
            if self.app.cmd.monitor(work_dir=os.path.dirname(run_info)):
                self.app.log.warn("Not running job")
                continue
            if self.pargs.restart:
                self.app.log.info("Removing old analysis files in {}".format(os.path.dirname(run_info)))
                remove_files(run_info, **vars(self.pargs))
            (cl, platform_args) = run_bcbb_command(run_info, **vars(self.pargs))
            self.app.cmd.command(cl, **{'platform_args':platform_args, 'saveJobId':True})
            os.chdir(orig_dir)

