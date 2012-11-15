"""Miscallaneous module"""
import sys
import os
import re
import contextlib
import itertools
import scilifelab.log

LOG = scilifelab.log.minimal_logger(__name__)

from cement.core.backend import minimal_logger
LOG = minimal_logger(__name__)

## yes or no: http://stackoverflow.com/questions/3041986/python-command-line-yes-no-input
def query_yes_no(question, default="yes", force=False):
    """Ask a yes/no question via raw_input() and return their answer.
    
    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning
    an answer is required of the user). The force option simply
    sets the answer to default.
    
    The "answer" return value is one of "yes" or "no".
    
    :param question: the displayed question
    :param default: the default answer
    :param force: set answer to default
    :returns: yes or no
    """
    valid = {"yes":True,   "y":True,  "ye":True,
             "no":False,     "n":False}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        if not force:
            choice = raw_input().lower()
        else:
            choice = "yes"
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                                 "(or 'y' or 'n').\n")

def walk(rootdir):
    """
    Perform a directory walk
    
    :param rootdir: Root directory of search

    :returns: List of files 
    """
    flist = []
    for root, dirs, files in os.walk(rootdir):
        flist = flist + [os.path.join(root, x) for x in files]
    return flist

def filtered_walk(rootdir, filter_fn, include_dirs=None, exclude_dirs=None, get_dirs=False): 
    """Perform a filtered directory walk.

    :param rootdir: Root directory
    :param filter_fn: Filtering function that returns boolean
    :param include_dirs: Only traverse these directories (list)
    :param exclude_dirs: Exclude these directories (list)

    :returns: Filtered file list 
    """
    flist = []
    dlist = []
    for root, dirs, files in os.walk(rootdir):
        if include_dirs and len(set(root.split(os.sep)).intersection(set(include_dirs))) == 0:
            ## Also try re.search in case we have patterns
            if re.search("|".join(include_dirs), root):
                pass
            else:
                continue
        if exclude_dirs and len(set(root.split(os.sep)).intersection(set(exclude_dirs))) > 0:
            continue
        if exclude_dirs and re.search("|".join(exclude_dirs), root):
            continue
        dlist = dlist + [os.path.join(root, x) for x in dirs]
        flist = flist + [os.path.join(root, x) for x in filter(filter_fn, files)]
    if get_dirs:
        return dlist
    else:
        return flist

def filtered_output(pattern, data):
    """
    Filter output
    
    :param pattern: a list or string of patterns
    :param data: a data list to filter

    :returns: filtered output
    """
    ## Sometimes read as string, sometimes as list...
    if type(pattern) == str:
        re_obj = re.compile(pattern.replace("\n", "|"))
    elif type(pattern) == list:
        re_obj = re.compile("|".join(pattern))

    def ignore(line):
        return re_obj.match(line) == None
    return filter(ignore, data)

def ls(path, pattern=None):
    """
    Perform ls on a path, possibly filtering output
    
    :param path: a path
    :param pattern: pattern to exclude 

    :returns: ls of path
    """
    ## Sometimes read as string, sometimes as list...
    if type(pattern) == str:
        re_obj = re.compile(pattern.replace("\n", "|"))
    elif type(pattern) == list:
        re_obj = re.compile("|".join(pattern))
    elif pattern is None:
        re_obj = None
    obj = os.listdir(path)
    def ignore(fn):
        if not re_obj:
            return False
        return re_obj.match(fn) != None
    return [x for x in obj if not ignore(x)]
    
## From bcbb
def safe_makedir(dname):
    """Make a directory if it doesn't exist, handling concurrent race conditions.
    """
    if not os.path.exists(dname):
        # we could get an error here if multiple processes are creating
        # the directory at the same time. Grr, concurrency.
        try:
            os.makedirs(dname)
        except OSError:
            if not os.path.isdir(dname):
                raise
    else:
        LOG.warning("Directory {} already exists; not making directory".format(dname))
    return dname

@contextlib.contextmanager
def chdir(new_dir):
    """Context manager to temporarily change to a new directory.

    http://lucentbeing.com/blog/context-managers-and-the-with-statement-in-python/
    """
    cur_dir = os.getcwd()
    # FIXME: currently assuming directory exists
    safe_makedir(new_dir)
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(cur_dir)

def opt_to_dict(opts):
    """Transform option list to a dictionary.

    :param opts: option list
    
    :returns: option dictionary
    """
    if isinstance(opts, dict):
        return
    args = list(itertools.chain.from_iterable([x.split("=") for x in opts]))
    opt_d = {k: True if v.startswith('-') else v
             for k,v in zip(args, args[1:]+["--"]) if k.startswith('-')}
    return opt_d
