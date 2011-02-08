#!/usr/bin/env python
# encoding: utf-8
'''
vc.py
VERCONT - The Version Controller
Copyright (c) 2008-2011 Szymon Wrozynski
Licensed under the MIT License
'''

import sys
import os
import time
import pickle
import getopt
from shutil import rmtree
import zlib

__version__ = '0.5.5'
__author__ = 'Szymon Wrozynski (c) 2008-2011'
__modelversion__ = '0.3.3'
__license__ = 'Licensed under the MIT License'

__all__ = (
    'NoSuchRevisionError',
    'NoSuchBranchError',
    'BranchExistsError',
    'NotDirectoryError',
    'BadDataError',
    'File',
    'Directory',
    'Revision',
    'Branch',
    'Repository',
    '__version__',
    '__author__',
    '__modelversion__'
)


# Repository model ###########################


class NoSuchRevisionError(Exception):
    def __init__(self, num):
        self.num = num
        

class NoSuchBranchError(Exception):
    def __init__(self, name):
        self.name = name
    
        
class BranchExistsError(Exception):
    def __init__(self, name):
        self.name = name
    
    
class NotDirectoryError(Exception):
    def __init__(self, path):
        self.path = path

        
class BadDataError(Exception):
    def __init__(self, filename, data):
        self.filename = filename
        self.data = data


class File(object):
    def __init__(self, name, prevfile, dr):
        self._data = None
        self.name = name
        self.mtime = 0
        self.dir = dr
        self.prevfile = prevfile
    
    def _getdata(self):
        if self._data is not None:
            return zlib.decompress(self._data)
        else:
            return None
    
    def _setdata(self, data):
        self._data = zlib.compress(data, zlib.Z_BEST_COMPRESSION)
        
    data = property(_getdata, _setdata)
        
    def __eq__(self, other):
        return self.data == other.data \
                and self.name == other.name \
                and self.path() == other.path()
    
    def __ne__(self, other):
        return not self.__eq__(other)
        
    def update(self, dest, callback=None):
        path = os.path.join(dest, self.name)
        f = open(path, 'wb')
        f.write(self.data)
        f.close()
        if callback: callback(self, path)
    
    def commit(self, dest, callback=None):
        path = os.path.join(dest, self.name)
        self.mtime = os.path.getmtime(path)
        f = open(path, 'rb')
        data = f.read()
        f.close()
        if self.prevfile and data == self.prevfile.data:
            self._data = self.prevfile._data
        else:
            self.data = data
        if callback: callback(self, path)
    
    def is_changed(self):
        return self.prevfile is None or self._data is not self.prevfile._data
        
    def visit(self, accept):
        accept(self)
    
    def path(self):
        return os.path.join(self.dir.path(), self.name)
    
    
class Directory(object):
    def __init__(self, name, prevdir, parent=None):
        self.name = name
        self.parent = parent
        self.files = {}
        self.dirs = {}
        self.prevdir = prevdir
    
    def __eq__(self, other):
        firstcheck = self.name == other.name \
                and len(self.files.keys()) == len(other.files.keys()) \
                and len(self.dirs.keys()) == len(other.dirs.keys()) \
                and self.path() == other.path()
        if not firstcheck: return False
        for fkey in self.files:
            of = other.files.get(fkey) # might be None
            if of is None or self.files[fkey] != of: return False
        for dkey in self.dirs:
            od = other.dirs.get(dkey) 
            if od is None or self.dirs[dkey] != od: return False
        return True
        
    def __ne__(self, other):
        return not self.__eq__(other)
        
    def path(self):
        if not self.parent:
            return self.name
        else:
            return os.path.join(self.parent.path(), self.name)
    
    def update(self, dest, callback=None):
        path = os.path.join(dest, self.name)
        if not os.path.isdir(path): os.mkdir(path)
        for name in self.files:
            self.files[name].update(path, callback)
        for name in self.dirs:
            self.dirs[name].update(path, callback)
        if callback: callback(self, path)
    
    def commit(self, dest, callback=None):                                            
        path = os.path.join(dest, self.name)
        for entry in os.listdir(path):
            entrypath = os.path.join(path, entry)
            if os.path.isdir(entrypath):
                if self.prevdir:
                    prevdir = self.prevdir.dirs.get(entry) # none otherwise
                else:
                    prevdir = None
                self.dirs[entry] = Directory(entry, prevdir, self)
                self.dirs[entry].commit(path, callback)
            else:
                if self.prevdir:
                    prevfile = self.prevdir.files.get(entry)
                else:
                    prevfile = None
                self.files[entry] = File(entry, prevfile, self)
                self.files[entry].commit(path, callback)
        if callback: callback(self, path)        
        
    def visit(self, accept):
        accept(self)
        for key in self.dirs:
            self.dirs[key].visit(accept)
        for key in self.files:
            self.files[key].visit(accept)
    
    def datasize(self):
        filessize = 0
        for key in self.files:
            if self.files[key].data:
                filessize += len(self.files[key].data)
        for key in self.dirs:
            if self.dirs[key]:
                filessize += self.dirs[key].datasize()
        return filessize


class Revision(object):
    def __init__(self, num, desc=None, prev=None):
        self.num = num
        self.desc = desc
        self.root = None
        self.prev = prev
        self.time = time.time()
        
    def commit(self, path, callback=None):
        parts = os.path.split(path)
        if self.prev:
            pvroot = self.prev.root
        else:
            pvroot = None
        self.root = Directory(parts[1], pvroot)
        self.root.commit(parts[0], callback)
        if callback: callback(self, path)
    
    def update(self, path, callback=None):
        parts = os.path.split(path)
        self.root.update(parts[0], callback)
        if callback: callback(self, path)
    
    def visit(self, accept):
        accept(self)
        self.root.visit(accept)
        
    def same_as_prev(self):
        return self.prev and self.root == self.prev.root
    
    def datasize(self):
        return self.root.datasize()


class Branch(object):
    def __init__(self, name, path):
        self.revisions = []
        self.name = name
        self._check_path(path)
        self.path = path
        
    def _set_path(self, path):
        if path.endswith(os.sep): path = path.rstrip(os.sep)
        self._path = path
        root = os.path.split(path)[1]
        for r in self.revisions:
            r.root.name = root
    
    def _get_path(self):
        return self._path
        
    path = property(_get_path, _set_path)
    
    def commit(self, desc=None, callback=None):
        self._check_path(self.path)
        num = len(self.revisions)
        if num == 0:
            v = Revision(num, desc)
        else:
            v = Revision(num, desc, self.revisions[-1])
        v.commit(self.path, callback)
        sthnew = not v.same_as_prev()
        if sthnew: self.revisions.append(v)
        return sthnew

    def update(self, num, callback=None):
        if not self.has_revision(num):
            raise NoSuchRevisionError(num)
        self.revisions[num].update(self.path, callback)

    def has_revision(self, num):
        return num in range(-len(self.revisions), len(self.revisions))

    def visit(self, accept):
        accept(self)
        for v in self.revisions:
            v.visit(accept)

    def _check_path(self, path):
        if not os.path.isdir(path): 
            raise NotDirectoryError(path)
    
        
class Repository(object):
    EXT = os.extsep + 'vcr'
    DEFAULT_BRANCH = 'trunk'
    
    def __init__(self, path, defbranch=None):
        if defbranch:
            self._defbranch = defbranch
        else:
            self._defbranch = Repository.DEFAULT_BRANCH
        self.branches = {self._defbranch: Branch(self._defbranch, path)}
        self.ver = __modelversion__
    
    def has_branch(self, branchname):
        return self.branches.has_key(branchname)
    
    def _check_branch(self, branchname):
        if not self.has_branch(branchname):
            raise NoSuchBranchError(branchname)
    
    def add_branch(self, branchname, path):
        if self.has_branch(branchname):
            raise BranchExistsError(branchname)
        self.branches[branchname] = Branch(branchname, path)
        
    def remove_branch(self, branchname):
         self._check_branch(branchname)
         del(self.branches[branchname])
         
    def update(self, revno, branchname=None, callback=None):
        if branchname:
            self._check_branch(branchname)
        else:
            branchname = self.defbranch
        self.branches[branchname].update(revno, callback)
    
    def commit(self, desc, branchname=None, callback=None):
        if branchname:
            self._check_branch(branchname)
        else:
            branchname = self.defbranch
        return self.branches[branchname].commit(desc, callback)
    
    def _set_defbranch(self, branchname):
        self._check_branch(branchname)
        self._defbranch = branchname
    
    def _get_defbranch(self):
        return self._defbranch
        
    defbranch = property(_get_defbranch, _set_defbranch)
    
    def save(self, name, path=os.getcwd()):
        if not name.endswith(Repository.EXT): name += Repository.EXT
        f = open(os.path.join(path, name), 'wb')
        try:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)
        finally:
            f.close()
    
    @classmethod
    def load(cls, name, path=os.getcwd()):
        if not name.endswith(cls.EXT): name += cls.EXT
        f = open(os.path.join(path, name), 'rb')
        try:
            repo = pickle.load(f)
        finally:
            f.close()
        if not isinstance(repo, Repository) or repo.ver != __modelversion__:
            raise BadDataError(name, repo)
        return repo
        
    
# User Interface #######################


class Usage(Exception): 
    def __init__(self, msg):
        self.msg = msg


def _check_repname():
    if not repname:
        raise Usage('provide the repository name or create a new one')

def _callback(sender, path):
    if isinstance(sender, Revision):
        print 'Revision %d processed.' % sender.num
    else:
        if isinstance(sender, File) and sender.is_changed():
            print 'Processing %s *' % path
        else:
            print 'Processing %s' % path

def _check_branchname(repo):
    global branchname
    if not branchname:
        branchname = repo.defbranch
    if not repo.has_branch(branchname):
        raise Usage('there is no branch named "%s" in repository "%s"' \
                % (branchname, repname))

def _check_ver(branch, num):
    if not branch.has_revision(num):
        raise Usage('branch "%s" does not have a revision no %d' \
                % (branch.name, num))
                
def _list_print(sender, vonly=False):
    if isinstance(sender, Revision):
        t = '%d-%d-%d, %d:%d:%d' % time.localtime(sender.time)[:-3]
        if vonly:
            print '%d\t%s\t%d\t%s' \
                    % (sender.num, t, sender.datasize(), sender.desc)
        else:
            print 'Repository:\t%s' % repname
            print 'Branch:\t\t%s' % branchname
            print 'Revision no %d from %s (%s):' \
                    % (sender.num, t, sender.desc)
    elif isinstance(sender, File) and not vonly:
        if (sender.is_changed()):
            print '  * %s' % sender.path()
        else:
            print '    %s' % sender.path()

def _vonly_print(sender):
    _list_print(sender, True)
    
def _parse_num(text):
    try:
        return int(text)
    except:
        raise Usage('revision number must be an integer')

def _load_repo(path):
    try:
        return Repository.load(repname, path)
    except IOError, e:
        if e.errno == 2:
            raise Usage('repository named "%s" not found' % repname)
        else:
            raise Usage('error no %d, file: %s: %s' \
                    % (e.errno, e.filename, e.strerr))
    except OSError, e:
        raise Usage('error no %d, file: %s: %s' \
                % (e.errno, e.filename, e.strerr))
    except pickle.PickleError, e:
        raise Usage('serialization problem: %s' % str(e))
    except BadDataError, e:
        if isinstance(e.data, Repository):
            raise Usage('bad data model version, should be: %s, was: %s' \
                    % (__modelversion__, e.data.ver))
        else:
            raise Usage('repository not found in the file "%s"' % e.filename)

def _process_nde(e, bname=None):
    dirmsg = 'does not point to a directory'
    if bname:
        raise Usage('the path "%s" of branch "%s" %s' \
                % (e.path, bname, dirmsg))
    else:
        raise Usage('the path "%s" %s' % (e.path, dirmsg))

def c_commit(args, path): 
    _check_repname()
    repo = _load_repo(path)
    _check_branchname(repo)
    if len(args) > 0: 
        desc = args[0]
    else:
        desc = None
    try:
        if repo.commit(desc, branchname, _callback):
            print 'Revision commited to the branch "%s" of repository "%s".' \
                    % (branchname, repname)
            repo.save(repname, path)
        else:
            print 'Nothing changed. Commit aborted.'
    except NotDirectoryError, e:
        _process_nde(e, branchname)
    
def c_list(args, path):
    _check_repname()
    repo = _load_repo(path)
    _check_branchname(repo)
    b = repo.branches[branchname]
    if len(args) > 0:
        num = _parse_num(args[0])
        _check_ver(b, num)
        b.revisions[num].visit(_list_print)
    else:
        bra = ''
        for k in repo.branches.keys():
            if k == b.name:
                bra += '[%s]' % k
            else:
                bra += '%s' % k
            bra += ', '
        print 'Repository:\t%s' % repname
        print 'Branches:\t' + bra.rstrip(', ')
        print 'Branch path:\t%s' % b.path
        print 'Revisions:'
        print 'No\tDate and time\t\tSize\t\tDescription'
        b.visit(_vonly_print)
    
def c_update(args, path):
    _check_repname()
    if len(args) != 1: 
        raise Usage('provide exactly 1 revision number')
    num = _parse_num(args[0])
    repo = _load_repo(path)
    _check_branchname(repo)
    b = repo.branches[branchname]
    _check_ver(b, num)
    isdir = os.path.isdir(b.path)
    if isdir: 
        has_backup = repo.commit('BACKUP', branchname)
        try:
            rmtree(b.path)
        except:
            print 'An error occured while removing.'
            print 'Restoring data from backup or last revision...'
            repo.update(-1, branchname) # if sth went wrong revert content
            raise
        else:
            if has_backup: repo.branches[branchname].revisions.pop() 
            # remove backup
    repo.update(num, branchname, _callback)
    if num >= 0:
        printednum = num
    else:
        printednum = len(repo.branches[branchname].revisions) + num
    print 'Data updated to revision %d of branch "%s".' \
            % (printednum, branchname)
    
def c_new(args, path):
    if not repname:
        raise Usage('provide a new repository name')
    try:
        repo = _load_repo(path)
    except Usage: #repo creation
        if len(args) != 1: 
            raise Usage('provide the path associated with the default branch')
        try:
            repo = Repository(args[0], branchname)
            repo.commit(None, branchname, _callback)
            _check_branchname(repo)
            repo.save(repname, path)
            print 'Repository "%s" has been created and saved.' % repname 
        except NotDirectoryError, e:
            _process_nde(e)
    else: #branch creation
        try:
            if not branchname:
                raise Usage('provide a new branch name')
            if len(args) > 0:
                monipath = args[0]
                defbname = None
            else:
                monipath = repo.branches[repo.defbranch].path
                defbname = repo.defbranch
            repo.add_branch(branchname, monipath)
            repo.commit(None, branchname, _callback)
            repo.save(repname, path)
            print 'Branch "%s" has been created and saved.' % branchname
            if defbname:
                pathmsg = ' from the default branch ("%s").' % defbname
            else:
                pathmsg = '.'
            print 'Created with path "%s"%s' % (monipath, pathmsg)
        except BranchExistsError, e:
            raise Usage('branch "%s" already exists' % e.name)
        except NotDirectoryError, e:
            _process_nde(e)

def c_del(args, path):
    if len(args) != 1:
        raise Usage('provide the name of the branch you want to delete')
    _check_repname()
    repo = _load_repo(path)
    if not repo.has_branch(args[0]):
        raise Usage('there is no branch "%s" in the repository "%s"' \
                % (args[0], repname))
    if args[0] == repo.defbranch:
        raise Usage('you cannot delete the default branch')
    del(repo.branches[args[0]])
    repo.save(repname, path)
    print 'Branch "%s" deleted.' % args[0]

def c_ren(args, path):
    if len(args) != 1:
        raise Usage("provide the new branch name")
    _check_repname()
    repo = _load_repo(path)
    _check_branchname(repo)
    has_default = repo.defbranch == branchname
    b = repo.branches[branchname]
    del(repo.branches[branchname])
    b.name = args[0]
    repo.branches[args[0]] = b
    if has_default:
        repo.defbranch = args[0]
    repo.save(repname, path)
    print 'Branch "%s" renamed to "%s".' % (branchname, args[0])
    
def c_def(args, path):
    if len(args) != 1:
        raise Usage('provide the new default branch name')
    _check_repname()
    repo = _load_repo(path)
    if not repo.has_branch(args[0]):
        raise Usage('there is no branch named "%s"' % args[0])
    repo.defbranch = args[0]
    repo.save(repname, path)
    print 'Default branch set to "%s".' % args[0]
        
def c_path(args, path): 
    _check_repname()
    if len(args) != 1:
        raise Usage('provide a new path')
    repo = _load_repo(path)
    _check_branchname(repo) 
    repo.branches[branchname].path = args[0]
    repo.save(repname, path)
    print 'The path of branch "%s" changed successfully.' % branchname

def c_desc(args, path):
    _check_repname()
    if len(args) != 2: 
        raise Usage('provide the revision number and the new description')
    repo = _load_repo(path)
    _check_branchname(repo)
    num = _parse_num(args[0])
    b = repo.branches[branchname]
    _check_ver(b, num)
    b.revisions[num].desc = args[1]
    repo.save(repname, path)
    print 'Description of revision %d of branch "%s" changed to:' \
            % (num, branchname)
    print args[1]

def c_help(args): 
    header = '\nVercont V.%s\n%s' % (__version__, __author__)
    help_message = '''
USAGE:
    [python] vc.py [OPTIONS] COMMAND [arguments]
        
OPTIONS:
    -r, --repo          Indicates a repository. May be ommited if there is
                        only one repository in the current directory.
    -b, --branch        Indicates a branch other than the default one.

COMMANDS:
    h, help             Prints this message.
    c, commit           Stores a new revision. Optional descriptions
                        may be passed as an argument.
    u, update           Updates data to the revision. The revision number
                        should be passed as the argument.
    l, list             Lists all revisions the current branch or details 
                        of the revision specified by passing its number 
                        as the argument.
    new                 Creates a new branch or a repository given as options. 
                        The monitored path should be passed as an argument.
                        If the branch is ommited while creating a repository, 
                        the name "trunk" is used by default.
    del                 Removes a branch. It cannot delete the default branch.
    ren                 Renames branch given via options to the one given 
                        as an argument.
    path                Change the monitored path of the branch passed 
                        in options. Requires the new path as an argument.
    desc                Changes the description of the revision passed 
                        as the firts argument. Requires the description 
                        as the second argument.
    def                 Sets the default branch for the repository. 
                        Branch name should be passed as an argument. 
    
EXAMPLES:
    vc.py -r docs new /home/username/documents
        - Creates a new repository called "docs" of 
          "/home/username/documents".
          It creates a file called "docs.vcr" in the current working 
          directory.
    vc.py c
        - Commits changes as a new revision to the default repository.
    vc.py -b trunk c
        - The same as above, but for a branch named "trunk"
    vc.py update -1
        - Restores data to the last revision from the default 
          repository and branch.
    vc.py u 0
        - Restores data to the first revision from the default repository 
          and branch.
    '''
    print header
    print __license__
    print help_message

def parse_commands(args, path):
    if len(args) < 1:
        raise Usage('command not specified')
    elif args[0] in ('h', 'help'):
        c_help(args[1:])
    elif args[0] in ('c', 'commit'):
        c_commit(args[1:], path)
    elif args[0] in ('u', 'update'):
        c_update(args[1:], path)
    elif args[0] in ('l', 'list'):
        c_list(args[1:], path)
    elif args[0] == 'new':
        c_new(args[1:], path)
    elif args[0] == 'del':
        c_del(args[1:], path)
    elif args[0] == 'ren':
        c_ren(args[1:], path)
    elif args[0] == 'path':
        c_path(args[1:], path)
    elif args[0] == 'desc':
        c_desc(args[1:], path)
    elif args[0] == 'def':
        c_def(args[1:], path)
    else:
        if args[0].startswith('-'):
            raise Usage("option unknown")
        else:
            raise Usage('command unknown')

def parse_options(args, path):
    try:
        opts, commands = getopt.getopt(args, "r:b:", ("repo=", "branch="))
    except getopt.error, msg:
        raise Usage(msg)
    for option, value in opts:
        if option in ('-b', '--branch'):
            global branchname
            branchname = value
        if option in ('-r', '--repo'):
            global repname
            repname = norm_repname(value)
    parse_commands(commands, path)

def default_repname(path):
    rname = None
    for entry in os.listdir(path):
        if entry.endswith(Repository.EXT): 
            if rname is None:
                if not os.path.isdir(os.path.join(path, entry)):
                    rname = entry
            else:
                return None
    return rname

def norm_repname(name):
    if name and not name.endswith(Repository.EXT): name += Repository.EXT
    return name
    
def main(argv=None):
    if argv is None:
        argv = sys.argv
    path = os.getcwd()
    global repname, branchname
    branchname = None
    repname = norm_repname(default_repname(path))
    try:
        parse_options(argv[1:], path)
    except Usage, err:
        sname = sys.argv[0].split('/')[-1]
        print >> sys.stderr,  '%s: %s' % (sname, str(err.msg))
        print >> sys.stderr, '\tfor help use "%s h"' % sname
        return 2

if __name__ == '__main__':
    sys.exit(main())
    

