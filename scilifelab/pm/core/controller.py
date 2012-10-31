"""Pm Controller Module"""
import os
import sys
import re

from cement.core import interface, handler, controller, backend

from scilifelab.pm.lib.help import PmHelpFormatter
from scilifelab.utils.misc import filtered_output, query_yes_no, filtered_walk

class AbstractBaseController(controller.CementBaseController):
    """
    This is an abstract base controller.

    All controllers should inherit from this class.
    """
    class Meta:
        pass

    def _setup(self, base_app):
        self._meta.arguments.append( (['-n', '--dry_run'], dict(help="dry_run - don't actually do anything", action="store_true", default=False)) )
        self._meta.arguments.append((['--force'], dict(help="force execution", action="store_true", default=False)))
        self._meta.arguments.append((['--verbose'], dict(help="verbose mode", action="store_true", default=False)))
        self._meta.arguments.append((['--java_opts'], dict(help="java options", action="store", default="Xmx3g")))
        self._meta.arguments.append((['--input_file'], dict(help="Run on specific input file", default=None)))
        super(AbstractBaseController, self)._setup(base_app)

        self.ignore = self.config.get("config", "ignore")
        self.shared_config = dict()

    def _process_args(self):
        pass

    ## Redefine _dispatch so that it also looks for function _process_args
    def _dispatch(self):
        """
        Takes the remaining arguments from self.app.argv and parses for a
        command to dispatch, and if so... dispatches it.
        
        """
        self._add_arguments_to_parser()
        self._parse_args()
        self._process_args()
        
        if not self.command:
            self.app.log.debug("no command to dispatch")
        else:    
            func = self.exposed[self.command]     
            self.app.log.debug("dispatching command: %s.%s" % \
                      (func['controller'], func['label']))

            if func['controller'] == self._meta.label:
                getattr(self, func['label'])()
            else:
                controller = handler.get('controller', func['controller'])()
                controller._setup(self.app)
                getattr(controller, func['label'])()

    def _not_implemented(self, msg=None):
        self.log.warn("FIXME: Not implemented yet")
        if msg != None:
            self.log.warn(msg)
        raise NotImplementedError

    def _obsolete(self, msg):
        self.app.log.warn("This function is obsolete.")
        self.app.log.warn(msg)

    def _check_pargs(self, pargs, msg=None):
        """Check that list of pargs are present"""
        for p in pargs:
            if not self.pargs.__getattribute__(p):
                self.app.log.warn("Required argument '{}' lacking".format(p))
                return False
        return True

    def _assert_config(self, section, label):
        """
        Assert existence of config label. If not present, require
        that the section/label be defined in configuration file.
        """
        if not self.config.has_section(section):
            self.log.error("no such section '{}'; please define in configuration file".format(section))
            sys.exit()
        config_dict = self.config.get_section_dict(section)
        if not config_dict.has_key(label):
            self.log.error("no section '{}' with label '{}' in config file; please define accordingly".format(section, label))
            sys.exit()
        elif config_dict[label] is None:
            self.log.error("config section '{}' with label '{}' set to 'None'; please define accordingly".format(section, label))
            sys.exit()
        
    ## FIXME: should this be moved?
    def _ls(self, path, filter_output=False):
        """List contents of path"""
        if not os.path.exists(path):
            self.app.log.info("No such path {}".format(path))
            return
        out = self.app.cmd.command(["ls", path])
        if filter_output:
            out = filtered_output(self.ignore, out)
        if out:
            self.app._output_data["stdout"].write(out.rstrip())

class AbstractExtendedBaseController(AbstractBaseController):
    """
    This is an abstract extended base controller.

    All extended controllers should inherit from this class. The main difference to the AbstractBaseController is that this controller adds arguments for compressing and cleaning. 

    """ 
    
    ## Why doesn't setting arguments work?
    class Meta:
        compress_opt = "-v"
        compress_prog = "gzip"
        compress_suffix = ".gz"
        file_pat = []
        include_dirs = []
        root_path = None
        path_id = None

    def _setup(self, base_app):
        self._meta.arguments.append((['--pbzip2'], dict(help="Use pbzip2 as compressing device", default=False, action="store_true")))
        self._meta.arguments.append((['--pigz'], dict(help="Use pigz as compressing device", default=False, action="store_true")))
        self._meta.arguments.append((['--sam'], dict(help="Workon sam files", default=False, action="store_true")))
        self._meta.arguments.append((['--bam'], dict(help="Workon bam files", default=False, action="store_true")))
        self._meta.arguments.append((['--fastq'], dict(help="Workon fastq files", default=False, action="store_true")))
        self._meta.arguments.append((['--fastqbam'], dict(help="Workon fastq-fastq.bam files", default=False, action="store_true")))
        self._meta.arguments.append((['--pileup'], dict(help="Workon pileup files", default=False, action="store_true")))
        self._meta.arguments.append((['--split'], dict(help="Workon *-split directories", default=False, action="store_true")))
        self._meta.arguments.append((['--tmp'], dict(help="Workon staging (tx) and tmp directories", default=False, action="store_true")))
        self._meta.arguments.append((['--txt'], dict(help="Workon txt files", default=False, action="store_true")))
        self._meta.arguments.append((['--glob'], dict(help="Workon freetext glob expression. CAUTION: using wildcard expressions will remove *everything* that matches.", default=None, action="store")))
        self._meta.arguments.append((['--move'], dict(help="Transfer file with move", default=False, action="store_true")))
        self._meta.arguments.append((['--copy'], dict(help="Transfer file with copy (default)", default=True, action="store_true")))
        self._meta.arguments.append((['--rsync'], dict(help="Transfer file with rsync", default=False, action="store_true")))
        super(AbstractExtendedBaseController, self)._setup(base_app)

    def _process_args(self):
        if self.command in ["compress", "decompress", "clean", "du"]:
            if not self._meta.path_id:
                self.app.log.warn("not running {} on root directory".format(self.command))
                sys.exit()
        elif self.command in ["ls"]:
            if not self._meta.path_id:
                self._meta.path_id = ""
        else:
            pass
        ## Setup file patterns to use
        if self.pargs.fastq:
            self._meta.file_pat += [".fastq", "fastq.txt", ".fq"]
        if self.pargs.pileup:
            self._meta.file_pat += [".pileup", "-pileup"]
        if self.pargs.txt:
            self._meta.file_pat += [".txt"]
        if self.pargs.fastqbam:
            self._meta.file_pat += ["fastq-fastq.bam"]
        if self.pargs.sam:
            self._meta.file_pat += [".sam"]
        if self.pargs.bam:
            self._meta.file_pat += [".bam"]
        if self.pargs.split:
            self._meta.file_pat += [".intervals", ".bam", ".bai", ".vcf", ".idx"]
            self._meta.include_dirs += ["realign-split", "variants-split"]
        if self.pargs.tmp:
            self._meta.file_pat += [".idx", ".vcf", ".bai", ".bam", ".idx", ".pdf"]
            self._meta.include_dirs += ["tmp", "tx"]
        if self.pargs.glob:
            self._meta.file_pat += [self.pargs.glob]
            
        ## Setup zip program
        if self.pargs.pbzip2:
            self._meta.compress_prog = "pbzip2"
        elif self.pargs.pigz:
            self._meta.compress_prog = "pigz"

        if self._meta.path_id:
            assert os.path.exists(os.path.join(self._meta.root_path, self._meta.path_id)), "no such folder '{}' in {} directory '{}'".format(self._meta.path_id, self._meta.label, self._meta.root_path)

        ## FIXME: Setup file search if clean, compress, or decompress.
        ## Idea: store files in list and filter later on

    @controller.expose(hide=True)
    def default(self):
        print self._help_text

    ## du
    @controller.expose(help="Calculate disk usage")
    def du(self):
        if not self._check_pargs(["project"]):
            return
        out = self.app.cmd.command(["du", "-hs", "{}".format(os.path.join(self._meta.root_path, self._meta.path_id))])
        if out:
            self.app._output_data["stdout"].write(out.rstrip())

    ## clean
    @controller.expose(help="Remove files")
    def clean(self):
        if not self._check_pargs(["project"]):
            return
        pattern = "|".join(["{}(.gz|.bz2)?$".format(x) for x in self._meta.file_pat])
        def clean_filter(f):
            if not pattern:
                return
            return re.search(pattern , f) != None

        flist = filtered_walk(os.path.join(self._meta.root_path, self._meta.path_id), clean_filter, include_dirs=self._meta.include_dirs)
        if len(flist) == 0:
            self.app.log.info("No files matching pattern '{}' found".format(pattern))
            return
        if len(flist) > 0 and not query_yes_no("Going to remove {} files ({}...). Are you sure you want to continue?".format(len(flist), ",".join([os.path.basename(x) for x in flist[0:10]])), force=self.pargs.force):
            return
        for f in flist:
            self.app.log.info("removing {}".format(f))
            self.app.cmd.safe_unlink(f)

    def _compress(self, pattern, label="compress"):
        def compress_filter(f):
            if not pattern:
                return
            return re.search(pattern, f) != None

        if self.pargs.input_file:
            flist = [self.pargs.input_file]
        else:
            flist = filtered_walk(os.path.join(self._meta.root_path, self._meta.path_id), compress_filter)

        if len(flist) == 0:
            self.app.log.info("No files matching pattern '{}' found".format(pattern))
            return
        if len(flist) > 0 and not query_yes_no("Going to {} {} files ({}...). Are you sure you want to continue?".format(label, len(flist), ",".join([os.path.basename(x) for x in flist[0:10]])), force=self.pargs.force):
            sys.exit()
        for f in flist:
            self.log.info("{}ing {}".format(label, f))
            self.app.cmd.command([self._meta.compress_prog, self._meta.compress_opt, "%s" % f], label, ignore_error=True, **{'workingDirectory':os.path.dirname(f), 'outputPath':os.path.join(os.path.dirname(f), "{}-{}-drmaa.log".format(label, os.path.basename(f)))})

    ## decompress
    @controller.expose(help="Decompress files")
    def decompress(self):
        """Decompress files"""
        if not self._check_pargs(["project"]):
            return
        self._meta.compress_opt = "-dv"
        if self.pargs.pbzip2:
            self._meta.compress_suffix = ".bz2"
        pattern = "|".join(["{}{}$".format(x, self._meta.compress_suffix) for x in self._meta.file_pat])
        self._compress(pattern, label="decompress")
        
    ## compress
    @controller.expose(help="Compress files")
    def compress(self):
        if not self._check_pargs(["project"]):
            return
        self._meta.compress_opt = "-v"
        pattern = "|".join(["{}$".format(x) for x in self._meta.file_pat])
        self._compress(pattern)

    def file_filter(f):
        if not pattern:
            return
        return re.search(pattern, f) != None

    ## ls
    @controller.expose(help="List root folder")
    def ls(self):
        if self._meta.path_id == "":
            self._ls(self._meta.root_path, filter_output=True)
        else:
            if self._meta.file_pat:
                pattern = "|".join(["{}$".format(x) for x in self._meta.file_pat])
                flist = filtered_walk(os.path.join(self._meta.root_path, self._meta.path_id), file_filter)
                if flist:
                    self.app._output_data["stdout"].write("\n".join(flist))
            else:
                self._ls(os.path.join(self._meta.root_path, self._meta.path_id))
        
class PmController(controller.CementBaseController):
    """
    Main Pm Controller.

    """
    class Meta:
        label = 'base'
        description = 'Project/pipeline management tools'
        arguments = [
            (['--config'], dict(help="print configuration", action="store_true")),
            (['--config-example'], dict(help="print configuration example", action="store_true")),
            ]

    def _setup(self, app_obj):
        # shortcuts
        super(PmController, self)._setup(app_obj)

    @controller.expose(hide=True)
    def default(self):
        if self.app.pargs.config:
            print "FIXME: show config"
        elif self.app.pargs.config_example:
            print """Configuration example: save as ~/.pm/pm.conf and modify at will.

    [config]
    ignore = slurm*, tmp*

    [archive]
    root = /path/to/archive

    [production]
    root = /path/to/production

    [log]
    level = INFO
    file = ~/log/pm.log

    [project]
    root = /path/to/projects
    repos = /path/to/repos
        """
        else:
            print self._help_text
