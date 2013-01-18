"""Pm Project module"""
import os
import sys
import re
import yaml

from cement.core import controller, hook
from scilifelab.pm.core.controller import AbstractExtendedBaseController, AbstractBaseController
from scilifelab.utils.misc import query_yes_no, filtered_walk, walk
from scilifelab.pm.lib.clean import purge_alignments
from scilifelab.bcbio.run import find_samples, setup_sample, remove_files, run_bcbb_command
from scilifelab.pm.core.bcbio import BcbioRunController

## Main project controller
class ProjectController(AbstractExtendedBaseController, BcbioRunController):
    """
    Functionality for project management.
    """
    class Meta:
        label = 'project'
        description = 'Manage projects'
        flowcelldir = None

    def _setup(self, base_app):
        super(ProjectController, self)._setup(base_app)
        base_app.args.add_argument('-g', '--git', help="Initialize git directory in repos and project gitdir", default=False, action="store_true")
        base_app.args.add_argument('--minfilesize', help="Min file size to keep (in bytes). Default 2000.", default=2000, action="store", type=int)
        group = base_app.args.add_argument_group('Project path group.', 'Options to restrict operations to certain paths.')
        group.add_argument('--finished', help="include finished project listing", action="store_true", default=False)
        group.add_argument('--intermediate', help="Work on intermediate data", default=False, action="store_true")
        group.add_argument('--data', help="Work on data folder", default=False, action="store_true")


    ## Remember: need to do argument processing here also for stacked controllers
    ## FIX ME: _process_args should be called in the stacked controller
    def _process_args(self):
        # setup project search space
        if self.app.pargs.finished:
            self._meta.project_root = self.app.config.get("project", "finished")
        else:
            self._meta.project_root = self.app.config.get("project", "root")

        # If rm function set intermediate
        if self.command == "rm":
            self.pargs.intermediate = True

        # Set root path for parent class
        self._meta.root_path = self._meta.project_root
        assert os.path.exists(self._meta.project_root), "No such directory {}; check your project config".format(self._meta.project_root)
        ## Set path_id for parent class
        if self.pargs.project:
            self._meta.path_id = self.pargs.project
            # Add intermediate or data
            if self.app.pargs.intermediate:
                if os.path.exists(os.path.join(self._meta.project_root, self._meta.path_id, "nobackup")):
                    self._meta.path_id = os.path.join(self._meta.path_id, "nobackup", "intermediate")
                else:
                    self._meta.path_id = os.path.join(self._meta.path_id, "intermediate")
            if self.app.pargs.data and not self.app.pargs.intermediate:
                if os.path.exists(os.path.join(self._meta.project_root, self._meta.path_id, "nobackup")):
                    self._meta.path_id = os.path.join(self._meta.path_id, "nobackup", "data")
                else:
                    self._meta.path_id = os.path.join(self._meta.path_id, "data")
        super(ProjectController, self)._process_args()
 
    ## init
    @controller.expose(help="Initalize project folder")
    def init(self):
        if self.pargs.project=="":
            return
        self.log.info("Initalizing project %s" % self.pargs.project)
        ## Create directory structure
        dirs = ["%s_git" % self.pargs.project, "data", "intermediate"]
        gitdirs = ["config", "sbatch", "doc", "lib"] 
        [self.safe_makedir(os.path.join(self._meta.project_root, self.pargs.project, x)) for x in dirs]
        [self.safe_makedir(os.path.join(self._meta.project_root, self.pargs.project, dirs[0], x)) for x in gitdirs]
        ## Initialize git if repos defined and flag set
        if self.config.get("project", "repos") and self.pargs.git:
            dirs = {
                'repos':os.path.join(self.config.get("project", "repos"), "current", self.pargs.project),
                'gitdir':os.path.join(self._meta.project_root, self.pargs.project, "%s_git" % self.pargs.project)
                    }
            self.safe_makedir(dirs['repos'])
            self.sh(["cd", dirs['repos'], "&& git init --bare"])
            self.sh(["cd", dirs['gitdir'], "&& git init && git remote add origin", dirs['repos']])

    def _flowcells(self):
        files = []
        if self.pargs.project:
            self._meta.flowcelldir = os.path.join(self._meta.project_root, self.pargs.project, "nobackup", "data")
            if not os.path.exists(self._meta.flowcelldir):
                self._meta.flowcelldir = os.path.join(self._meta.project_root, self.pargs.project,"data")
            if not os.path.exists(self._meta.flowcelldir):
                return []
            files = os.listdir(self._meta.flowcelldir)
        return files
        
    ## NOTE: this is a temporary workaround for cases where data has
    ## been removed from production directory
    @controller.expose(help="Transfer project data to customer. Temporary fix for cases where data has been removed from production directory.")
    def transfer(self):
        if not self.pargs.flowcell:
            self.log.warn("No flowcellid provided. Please provide a flowcellid from which to deliver. Available options are:\n\t{}".format("\n\t".join(self._flowcells())))
            return
    
    ## purge_alignments
    @controller.expose(help="purge alignments in project folders")
    def purge(self):
        """Cleanup sam and bam files. In some cases, sam files
        persist. If the corresponding bam file exists, replace the sam
        file contents with a message that the file has been removed to
        save space.
        """
        if not self._check_pargs(["project"]):
            return
        if self.app.pargs.sam:
            purge_alignments(path=os.path.join(self._meta.root_path, self._meta.path_id), dry_run=self.app.pargs.dry_run, force=self.app.pargs.force, fsize=self.app.pargs.minfilesize)
        else:
            purge_alignments(path=os.path.join(self._meta.root_path, self._meta.path_id), dry_run=self.app.pargs.dry_run, force=self.app.pargs.force, ftype="bam", fsize=self.app.pargs.minfilesize)

class ProjectRmController(AbstractBaseController):
    class Meta:
        label = 'projectrm'
        description = 'Functionality for removing analyses from intermediate folders'
        arguments = [
            (['analysis_id'], dict(help="analysis name in intermediate", action="store", default=None, nargs="?", type=str)),
            ]
        stacked_on = 'project'

    @controller.expose(help="Remove analyses from project intermediate subfolder")
    def rm(self):
        if not self._check_pargs(["project",  "analysis_id"]):
            return
        indir = os.path.join(self.app.controller._meta.project_root, self.app.controller._meta.path_id, self.pargs.analysis_id)
        assert os.path.exists(indir), "No such analysis {} for project {}".format(self.pargs.analysis_id, self.pargs.project)
        try:
            flist = walk(indir)
        except IOError as e:
            self.app.log.warn(str(e))
            raise e
        if len(flist) > 0 and not query_yes_no("Going to remove all contents ({} files) of analysis {} for project {}... Are you sure you want to continue?".format(len(flist), self.pargs.analysis_id, self.pargs.project), force=self.pargs.force):
            return
        for f in flist:
            self.app.cmd.safe_unlink(f)
        self.app.log.info("removing {}".format(indir))
        self.app.cmd.safe_rmdir(indir)
