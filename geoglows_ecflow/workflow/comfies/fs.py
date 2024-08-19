#!/usr/bin/env python

"""
Copyright 2024 ecmwf

Licensed under the Apache License, Version 2.0 (the "License")

http://www.apache.org/licenses/LICENSE-2.0
"""

import os
import sys
import errno
import re
import subprocess
import shutil
import datetime
import argparse
import logging as log

class Error(Exception): pass
class NoMatchError(Error): pass
class FilesystemError(Error): pass

def _s2d(s):
    for pattern in ('%Y%m%d', '%Y%m%d%H'):
        try:
            d = datetime.datetime.strptime(s, pattern)
        except ValueError:
            continue
        return d
    raise ValueError('invalid date string ({})'.format(s))


class FileUtils(object):

    def mcopy(self, sources, dest):
        for src in sources:
            self.copy(src, dest)

    def match(self, directory, patterns):
        log.info('in match')
        items = self.readdir(directory)
        items.sort()
        matches = set()
        for pattern in patterns:
            found = False
            for item in items:
                path = os.path.join(directory, item)
                if re.search(pattern, path):
                    matches.add(path)
                    found = True
            if found == False:
                raise NoMatchError("no match for {}".format(pattern))
        paths = list(matches)
        paths.sort()
        return paths

    def last_match(self, directory, patterns):
        items = self.readdir(directory)
        items.sort()
        matches = {}
        for item in items:
            path = os.path.join(directory, item)
            for pattern in patterns:
                if re.search(pattern, path):
                    matches[pattern] = path
        for pattern in patterns:
            if pattern not in matches:
                raise NoMatchError("no match for {}".format(pattern))
        paths = matches.values()
        paths.sort()
        return paths

    def cp_match(self, srcdir, destdir, patterns):
        paths = self.match(srcdir, patterns)
        for path in paths:
            self.copy(path, destdir)

    def cp_last_match(self, srcdir, destdir, patterns):
        paths = self.last_match(srcdir, patterns)
        for path in paths:
            self.copy(path, destdir)

    def find_old(self, directory, refdate, patterns):
        items = self.readdir(directory)
        matches = []
        for patstr in patterns:
            pattern = re.compile(patstr)
            if pattern.groups == 0:
                raise NoMatchError(
                        "pattern {} must contain at least one "
                        "capturing group".format(patstr))
            for item in items:
                path = os.path.join(directory, item)
                match = pattern.search(path)
                if not match:
                    continue
                datestr = ''.join(
                        [match.group(i + 1) for i in range(0, pattern.groups)])
                try:
                    d = _s2d(datestr)
                except ValueError as e:
                    raise NoMatchError(
                            "cannot convert {datestr} extracted from {path} "
                            "with pattern {patstr} to a date".format(
                            datestr=datestr, path=path, patstr=patstr))
                if d < refdate:
                    matches.append(item)
        return matches

    def rm_old_files_bak(self, directory, refdate, patterns):
        old_items = self.find_old(directory, refdate, patterns)
        self.chdir(directory)
        for item in old_items:
            self.unlink(item)

    def rm_old_files(self, top, refdate, patterns):
        cpatterns = []
        for pattern in patterns:
            cpattern = re.compile(pattern)
            if cpattern.groups == 0:
                raise NoMatchError(
                        "pattern {} must contain at least one "
                        "capturing group".format(pattern))
            cpatterns.append(re.compile(pattern))
        for root, dirs, files in os.walk(top, topdown = False):
            for name in files:
                path = os.path.join(root, name)
                for cpattern in cpatterns:
                    match = cpattern.search(path)
                    if not match:
                        continue
                    datestr = ''.join(
                            [match.group(i + 1) for i in range(0, cpattern.groups)])
                    try:
                        filedate = _s2d(datestr)
                    except ValueError as e:
                        raise NoMatchError(
                                "cannot convert {datestr} extracted from {path} "
                                "with pattern {pattern} to a date".format(
                                datestr=datestr, path=path, pattern=cpattern.pattern))
                    if filedate < refdate:
                        self.unlink(path)
            for name in dirs:
                path = os.path.join(root, name)
                if self.rm_dir_if_empty(path, verbose=False):
                    log.info("removed empty {}".format(path))

    def rm_dir_if_empty(self, directory, verbose=True):
        try:
            self.rmdir(directory, verbose=verbose)
        except OSError as e:
            if e.errno == errno.ENOTEMPTY:
                if verbose:
                    log.info("directory {} not empty".format(directory))
                return False
            else:
                raise
        return True


class UnixUtils(FileUtils):

    def readdir(self, directory):
        try:
            items = os.listdir(directory)
        except OSError as e:
            raise FilesystemError(str(e))
        return items

    def copy(self, src, dest):
        try:
            shutil.copy(src, dest)
        except OSError as e:
            raise FilesystemError(str(e))

    def unlink(self, path):
        try:
            log.info("removing {}".format(path))
            os.unlink(path)
        except OSError as e:
            raise FilesystemError(str(e))

    def rmdir(self, directory, verbose=True):
        if verbose:
            log.info("removing {}".format(directory))
        os.rmdir(directory)

    def chdir(self, directory):
        os.chdir(directory)


class ECFSUtils(FileUtils):

    def readdir(self, directory):
        try:
            output = subprocess.check_output(['els', directory])
        except subprocess.CalledProcessError:
            raise FilesystemError('els {} failed.'.format(directory))
        return output.splitlines()

    def copy(self, src, dest):
        try:
            output = subprocess.check_output(['ecp', src, dest])
        except subprocess.CalledProcessError:
            raise FilesystemError('ecp {} {} failed.'.format(src, dest))

    def unlink(self, path):
        raise NotImplementedError()

    def rmdir(self, directory):
        raise NotImplementedError()

    def chdir(self, directory):
        raise NotImplementedError()

    def cached_mcopy(self, sources, dest, cache_root):
        srcdirs = {}
        unix = UnixUtils()
        for src in sources:
            dirname, filename = os.path.split(src)
            dirname = dirname[len('ec:/'):]
            if dirname not in srcdirs:
                srcdirs[dirname] = set()
            srcdirs[dirname].add(filename)
        for dirname in srcdirs:
            try:
                cached = set(unix.readdir(os.path.join(cache_root, dirname)))
            except FilesystemError:
                cached = set()
            uncached = srcdirs[dirname] - cached
            uncached_paths = [os.path.join('ec:', dirname, x) for x in uncached]
            # TODO: bulk copy rather than calling 'ecp' for each file.
            self.mcopy(uncached_paths, os.path.join(cache_root, dirname) + '/')



def ensure_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def expand_path(path):
    return os.path.expanduser(os.path.expandvars(path))

def real_path(path):
    return os.path.realpath(expand_path(path))


# python interface ------------------------------------------

def _make_fileutils(path):
    if path.find('ec:', 0) != -1:
        return ECFSUtils()
    else:
        return UnixUtils()

def match(directory, patterns):
    fileutils = _make_fileutils(directory)
    return fileutils.match(directory, patterns)

def last_match(directory, patterns):
    fileutils = _make_fileutils(directory)
    return fileutils.last_match(directory, patterns)

def cp_match(srcdir, destdir, patterns):
    fileutils = _make_fileutils(srcdir)
    fileutils.cp_match(srcdir, destdir, patterns)

def cp_last_match(srcdir, destdir, patterns):
    fileutils = _make_fileutils(srcdir)
    fileutils.cp_last_match(srcdir, destdir, patterns)

def find_old(directory, refdate, patterns):
    fileutils = _make_fileutils(directory)
    return fileutils.find_old(directory, refdate, patterns)

def rm_old_files(directory, refdate, patterns):
    fileutils = _make_fileutils(directory)
    fileutils.rm_old_files(directory, refdate, patterns)

def rm_dir_if_empty(directory):
    fileutils = _make_fileutils(directory)
    fileutils.rm_dir_if_empty(directory)


# commandline interface -------------------------------------

def _set_logging(verbose):
    if verbose == 1:
        level = log.INFO
    elif verbose >= 2:
        level = log.DEBUG
    else:
        level = None
    if verbose > 0:
        log.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=level)

def  _parse_args(parser, argv):
    # add arguments which are common to all commands
    parser.add_argument('-v', dest='verbose', action='count', default=0, help='verbose (-vv = more verbose)')
    args = parser.parse_args(argv)
    _set_logging(args.verbose)
    return args

def _fail(command, exception):
    sys.stderr.write(command + ': ' + str(exception) + '\n')
    return 1

def match_command(argv):
    parser = argparse.ArgumentParser(description='list all files matching patterns', prog='match')
    parser.add_argument('directory', metavar='DIR', help='directory to search')
    parser.add_argument('patterns', metavar='PATTERN', nargs='+', help='regular expression')
    args = _parse_args(parser, argv)
    try:
        paths = match(directory=args.directory, patterns=args.patterns)
    except Error as e:
        return _fail('match', e)
    for path in paths:
        print(path)

def last_match_command(argv):
    parser = argparse.ArgumentParser(description='list last match for every pattern', prog='last_match')
    parser.add_argument('directory', metavar='DIR', help='directory to search')
    parser.add_argument('patterns', metavar='PATTERN', nargs='+', help='regular expression')
    args = _parse_args(parser, argv)
    try:
        paths = last_match(directory=args.directory, patterns=args.patterns)
    except Error as e:
        return _fail('last_match', e)
    for path in paths:
        print(path)

def cp_match_command(argv):
    parser = argparse.ArgumentParser(description='copy matching files', prog='cp_match')
    parser.add_argument('srcdir', metavar='SRCDIR', help='source directory')
    parser.add_argument('destdir', metavar='DESTDIR', help='destination directory')
    parser.add_argument('patterns', metavar='PATTERN', nargs='+', help='regular expression')
    args = _parse_args(parser, argv)
    try:
        cp_match(srcdir=args.srcdir, destdir=args.destdir, patterns=args.patterns)
    except Error as e:
        return _fail('cp_match', e)

def cp_last_match_command(argv):
    parser = argparse.ArgumentParser(description='copy last match for each PATTERN', prog='cp_last_match')
    parser.add_argument('srcdir', metavar='SRCDIR', help='source directory')
    parser.add_argument('destdir', metavar='DESTDIR', help='destination directory')
    parser.add_argument('patterns', metavar='PATTERN', nargs='+', help='regular expression')
    args = _parse_args(parser, argv)
    try:
        cp_last_match(srcdir=args.srcdir, destdir=args.destdir, patterns=args.patterns)
    except Error as e:
        return _fail('cp_last_match', e)

def find_old_command(argv):
    parser = argparse.ArgumentParser(description='find files older than given date', prog='find_old')
    parser.add_argument('directory', metavar='DIR', help='directory to search')
    parser.add_argument('refdate', metavar='DATE', help='reference date')
    parser.add_argument('patterns', metavar='PATTERN', nargs='+', help='regular expression')
    args = _parse_args(parser, argv)
    try:
        paths = find_old(directory=args.directory, refdate=refdate, patterns=args.patterns)
    except Error as e:
        return _fail('find_old', e)
    for path in paths:
        print(path)

def rm_old_files_command(argv):
    parser = argparse.ArgumentParser(description='delete files older than given date', prog='rm_old_files')
    parser.add_argument('directory', metavar='DIR', help='directory with files to delete')
    parser.add_argument('refdate', metavar='DATE', help='reference date')
    parser.add_argument('patterns', metavar='PATTERN', nargs='+', help='regular expression')
    args = _parse_args(parser, argv)
    refdate = _s2d(args.refdate)
    try:
        rm_old_files(directory=args.directory, refdate=refdate, patterns=args.patterns)
    except Error as e:
        return _fail('rm_old_files', e)

def rm_dir_if_empty_command(argv):
    parser = argparse.ArgumentParser(description='remove directory if empty', prog='rm_dir_if_empty')
    parser.add_argument('directory', metavar='DIR', help='directory')
    args = _parse_args(parser, argv)
    try:
        rm_dir_if_empty(directory=args.directory)
    except Error as e:
        return _fail('rm_dir_if_empty', e)

def main():
    command_handlers = {
            'match': match_command,
            'last_match': last_match_command,
            'cp_match': cp_match_command,
            'cp_last_match': cp_last_match_command,
            'find_old': find_old_command,
            'rm_old_files': rm_old_files_command,
            'rm_dir_if_empty': rm_dir_if_empty_command,
            }
    parser = argparse.ArgumentParser(description='file utilities', prog='fileutils')
    parser.add_argument('command', choices=command_handlers.keys(), help='command')
    parser.add_argument('args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    # dispatch to appropriate command handler
    sys.exit(command_handlers[args.command](args.args))

if __name__ == "__main__":
    main()