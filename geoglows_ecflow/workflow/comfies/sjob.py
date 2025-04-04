#!/usr/bin/env python

"""
Copyright 2024 ecmwf

Licensed under the Apache License, Version 2.0 (the "License")

http://www.apache.org/licenses/LICENSE-2.0
"""

import os
import sys
import json
import errno
import uuid
import shlex
import getpass
import socket
import re
import logging as log

try:
    from collections.abc import OrderedDict  # noqa
except ImportError:
    from collections import OrderedDict  # noqa

import argparse
import subprocess
from subprocess import CalledProcessError
import codecs
import pkgutil


class ConfigError(Exception):
    pass


# -------------------------------------
# reader for the job destination config
# -------------------------------------


class JobDestFileDB(object):

    def __init__(self):
        self._user_config_dirs = [
            os.path.expanduser('~/.sjob'),
            os.path.expanduser('~/.comfies/sjob'),
            os.path.join(os.path.dirname(__file__), 'data', 'sjob')
        ]

    def read(self, jobdest):
        cfg = None
        # first, try user's config dirs
        for d in self._user_config_dirs:
            path = os.path.join(d, jobdest)
            try:
                f = open(path)
            except IOError:
                continue
            else:
                with f:
                    cfg = json.load(f)
                    cfg['__path__'] = path

        if cfg is None:
            # try to get config from the installed 'comfies' package
            try:
                s = pkgutil.get_data('geoglows_ecflow.workflow.comfies', 'data/sjob/' + jobdest)
            except IOError:
                # 'comfies' package is available but config file does not exist
                pass
            else:
                if s is not None:
                    cfg = json.loads(s)
                    cfg['__path__'] = os.path.join(
                            os.path.dirname(sys.modules['comfies'].__file__),
                            'data', 'sjob', jobdest)
                else:
                    # comfies package not available - ignore
                    pass

        if cfg is not None:
            return cfg

        raise ConfigError('could not find config file '
                'for destination "{}" in {}'.format(
            jobdest, ', '.join(self._user_config_dirs)))


# ------------------------------
# job info database
# ------------------------------


class JobInfoDB(object):
    """
    interface of a job info database.
    """

    def read(self, jobid):
        """ returns job info dictionary for given jobid """
        raise NotImplementedError()

    def write(self, dct):
        """ stores job info dictionary, returns jobid """
        raise NotImplementedError()

    def delete(self, jobid):
        """ removes job info from the database """
        raise NotImplementedError()



class JobInfoFileDB(JobInfoDB):

    """
    This class implements storing job info as
    ~/.comfies/sjob/jobs/{jobid} files
    where jobid is a unique UUID string.
    Currently not used as there is no
    reliable way to delete these files once
    a job stops running.
    """

    def __init__(self, path=os.path.expanduser('~/.comfies/sjob/jobs')):
        self._path = path

    def read(self, jobid):
        path = os.path.join(self._path, jobid)
        with open(path, 'r') as f:
            return json.load(f)

    def write(self, dct):
        jobid = uuid.uuid4().hex
        path = os.path.join(self._path, jobid)
        try:
            os.makedirs(self._path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        if os.path.isfile(path):
            raise Exception('job {} already exists'.format(jobid))
        with open(path, 'w') as f:
            json.dump(dct, f)
        return jobid

    def delete(self, jobid):
        path = os.path.join(self._path, jobid)
        os.unlink(path)



class JobInfoSubFileDB(JobInfoDB):

    """
    write() method prints job info to stdout.
    read() method reads job info from stdin.
    This JobInfoDB implementation is intended
    for reading/writing job info embedded in
    the job submission log files (*.sub files):
    """

    def read(self, jobid):
        sublog_path = jobid
        log.info('reading job info from ' + sublog_path)
        jobinfo = {}
        with open(sublog_path, 'r') as f:
            for line in f:
                match = re.match(r'JOBINFO (\S+) (.*)', line)
                if match is None:
                    continue
                key, value = match.groups()
                jobinfo[key] = value
        return jobinfo

    def write(self, dct):
        sublog_path = dct['script_path'] + '.sub'
        jobid = sublog_path
        line = 'JOBINFO {} {}'
        lines = [line.format(k, v) for k, v in dct.items()]
        jobinfo_str = '\n'.join(lines)
        # we simply print the jobinfo and assume that
        # stdout of the sjob is redirected to .sub logfile.
        print(jobinfo_str)
        sys.stdout.flush()
        return jobid

    def delete(self, jobid):
        pass

# ------------------------------
# script reader
# ------------------------------


def readlines(path):
    with codecs.open(path, 'r', encoding='utf-8') as f:
        return f.readlines()


# ------------------------------------------------------
# host operations, implement .execute() and .put() methods
# ------------------------------------------------------


def _execute(args, stdin_str=None):
    """
    Execute argv (command+arguments) redirecting
    stdin_str to the standard input of the command;
    return command's stdout and stderr.
    """
    log.info(' '.join(args))
    if stdin_str is None:
        stdin = None
    else:
        stdin = subprocess.PIPE
    process = subprocess.Popen(
            args, stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
            )
    if stdin_str is not None:
        stdin_str = stdin_str.encode('utf-8')
    stdout, unused_err = process.communicate(stdin_str)
    retcode = process.poll()
    stdout_str = stdout.decode('utf-8')
    if retcode:
        msg = 'Command {} returned non-zero exit status {}'
        msg = msg.format(' '.join(args), retcode)
        if stdout:
            msg += '. Output was:\n{}'.format(stdout_str)
        raise Host.CommandError(msg)
        #raise CalledProcessError(retcode, args, output=output)
    return stdout_str



class Host(object):

    class CommandError(Exception):
        pass



class LocalHost(Host):

    def execute(self, cmd, stdin_str=None):
        args = shlex.split(cmd)
        return _execute(args, stdin_str)

    def put(self, src, dest):
        args = ['cp', src, dest]
        log.info(' '.join(args))
        subprocess.check_call(args)



class SshHost(Host):

    def __init__(self, name, user=getpass.getuser(), port=22):
        self.name = name
        self.user = user
        self.port = port

    def execute(self, cmd, stdin_str=None):
        ssh_cmd = 'ssh -x -o ConnectTimeout=180 -o BatchMode=yes'
        ssh_cmd += ' -o StrictHostKeyChecking=no -p {} -l {} {}'
        ssh_cmd = ssh_cmd.format(self.port, self.user, self.name)
        strangechar = chr(167) # so that we don't have to escape quotes in cmd
        ssh_cmd += """ {quote}{cmd}{quote}""".format(cmd=cmd, quote=strangechar)
        lexer = shlex.shlex(ssh_cmd, posix=True)
        lexer.quotes = strangechar
        lexer.whitespace_split = True
        args = [arg for arg in lexer]
        return _execute(args, stdin_str)

    def put(self, src, dest):
        args = ['scp', '-P', str(self.port), src]
        args += ['{}@{}:{}'.format(self.user, self.name, dest)]
        log.info(' '.join(args))
        subprocess.check_call(args)



def host_factory(target_hostname, source_hostname=socket.getfqdn()):
        if target_hostname in (source_hostname, 'localhost'):
            return LocalHost()
        else:
            return SshHost(target_hostname)


class SubmissionError(Exception):
    pass


# ------------------------------
# JobDest base classes
# ------------------------------

class JobDest(object):

    def __init__(self,
            name,
            destdb = JobDestFileDB(),
            jobdb = JobInfoSubFileDB(),
            host_factory = host_factory,
            readlines=readlines
            ):
        self._name = name
        self._destdb = destdb
        self._jobdb = jobdb
        self._host_factory = host_factory
        self._readlines = readlines
        # get job destination parameters
        self._params = destdb.read(name)
        log.info('"{}" is {}'.format(
            name, self._params['__path__']))
        self._hostname = self._params['hostname']
        self._default_shell = self._params['default_shell']
        self._host = host_factory(self._hostname)
        self._directives = []  #set by the BatchJobDest subclasses

    def _parse_id(self, text):
        """
        Get job ID from output of the job submission command.
        Concrete job classes must provide the ._id_regex pattern.
        """
        match = re.match(self._id_regex, text, re.MULTILINE | re.DOTALL)
        if match is None:
            msg = '{} does not match {}'
            msg = msg.format(text, self._id_regex)
            raise SubmissionError(msg)
        return match.group(1)

    def generate_job(self, script_path, output_path):
        # read script
        script = self._readlines(script_path)
        script_iter = iter(script)
        job = []
        script_is_empty = False
        script_has_shebang = False
        try:
            first_line  = next(script_iter)
        except StopIteration:
            script_is_empty = True
        # check the first line for shebang
        if not script_is_empty and first_line[:2] == '#!':
            script_has_shebang = True
            interpreter_directive = first_line[2:].strip()
        else:
            interpreter_directive = self._default_shell
        job.append('#!' + interpreter_directive + '\n')
        # from shell directive get shell path and shell args
        shell_ = interpreter_directive.split(None, 1) + ['']
        shell_path = shell_[0]
        shell_args = shell_[1]
        # generate job directives
        script_dir = os.path.dirname(script_path)
        script_name = os.path.basename(script_path)
        script_parent = os.path.basename(script_dir)
        for directive in self._directives:
            directive = directive.format(
                # these parameters are available
                # for use in a directive string:
                output_path = output_path,
                script_name = script_name,
                script_parent = script_parent,
                shell_path = shell_path,
                shell_args = shell_args
                )
            job.append(directive + '\n')
        if not script_is_empty:
            if not script_has_shebang:
                job.append(first_line)
            for line in script_iter:
                job.append(line)
        return ''.join(job)

    def kill(self, jobid):
        jobinfo = self._jobdb.read(jobid)
        native_jobid = jobinfo['native_jobid']
        log.info('Deleting job {}'.format(jobid))
        output = self._host.execute("sh -l -c '{} {}'".format(self._kill_cmd, native_jobid))
        log.info('{} request sent'.format(self._kill_cmd))

    def stat(self, jobid):
        jobinfo = self._jobdb.read(jobid)
        native_jobid = jobinfo['native_jobid']
        log.info('Getting Job Status {}'.format(jobid))
        try:
            output = self._host.execute(self._cmd +"{} {}'".format(self._stat_cmd, native_jobid))
        except Host.CommandError:
            log.warn('Job not found on remote host')


class BatchJobDest(JobDest):

    def __init__(self,
            name,
            destdb = JobDestFileDB(),
            jobdb = JobInfoSubFileDB(),
            host_factory = host_factory,
            readlines = readlines
            ):
        super(BatchJobDest, self).__init__(
            name, destdb, jobdb, host_factory, readlines)
        self._directives = self._params['directives']

    def submit(self, script_path, output_path):
        cmd = self._cmd
        output_dir = os.path.dirname(output_path)
        if output_dir:
            cmd += "mkdir -p {output_dir}; chmod 755 {output_dir}; "
        cmd += "{submit_cmd}'"
        cmd = cmd.format(output_dir=output_dir, submit_cmd=self._submit_cmd)
        job = self.generate_job(script_path, output_path)
        output = self._host.execute(cmd, job)
        log.info('{} request sent.'.format(self._submit_cmd))
        native_jobid = self._parse_id(output.rstrip())

        # store job info (for kill command)
        jobinfo = dict(
                dest = self._name,
                native_jobid = native_jobid,
                script_path = script_path,
                output_path = output_path
                )
        jobid = self._jobdb.write(jobinfo)
        if os.path.dirname(output_path) != os.path.dirname(script_path):
            self._host.put(script_path, os.path.dirname(output_path))
            if os.path.exists(script_path+'.jobinfo'):
                self._host.put(script_path+'.jobinfo', os.path.dirname(output_path))
        return jobid

# -----------------------------
# JobDest concrete classes
# -----------------------------


class Executable(JobDest):

    def __init__(self,
            name,
            destdb = JobDestFileDB(),
            jobdb = JobInfoSubFileDB(),
            host_factory = host_factory,
            readlines = readlines
            ):
        super(Executable, self).__init__(
            name, destdb, jobdb, host_factory, readlines)
        self._directives = self._params['directives']

    def submit(self, script_path, output_path):
        output_dir = os.path.dirname(output_path)
        script_name = os.path.basename(script_path)
        exe_path = os.path.join(output_dir, script_name + '.exe')
        cmd = "sh -c 'cat > {exe}; chmod 755 {exe}; "
        cmd += "{exe} > {output_path} 2>&1 & echo PID: $!'"
        cmd = cmd.format(exe=exe_path, output_path=output_path)
        job = self.generate_job(script_path, output_path)
        output = self._host.execute(cmd, job)
        native_jobid = self._parse_id(output)
        # store job info (for 'kill' command)
        jobinfo = dict(
            dest = self._name,
            native_jobid = native_jobid,
            script_path = script_path,
            output_path = output_path
            )
        jobid = self._jobdb.write(jobinfo)
        log.info('Script executed.')
        return jobid

    _id_regex = r'^PID: ([0-9]+)'
    _kill_cmd = r'kill -TERM'



class Slurm(BatchJobDest):

    _submit_cmd = 'sbatch'
    _stat_cmd = 'squeue -j'
    _kill_cmd = 'scancel'
    _id_regex = r'.*Submitted batch job ([0-9]+)$'
    _cmd = "sh -l -c '"

class Sge(BatchJobDest):

    _submit_cmd = 'squeue -notify'
    _stat_cmd = 'qstat'
    _kill_cmd = 'qdel'
    _id_regex = r'.*Your job ([0-9]+) .*'
    _cmd = "sh -l -c '"

class EcPbs(BatchJobDest):

    _submit_cmd = 'qsub'
    _stat_cmd = 'ec_qstat -f'
    _kill_cmd = 'ec_qdel'
    _id_regex = r'^([0-9]+\.[a-z]{5}\w)$'
    _cmd = "sh -c '"

# ------------------------------
# top level functions
# ------------------------------


def _create_jobdest(jobdest_name):
    """
    Create jobdest object based on the "class"
    jobdest configuration parameter.
    """
    jobdestdb = JobDestFileDB()
    jobdest_config = jobdestdb.read(jobdest_name)
    jobdest_class_name = jobdest_config['class']
    try:
        JobDestClass = globals()[jobdest_class_name]
    except KeyError:
        msg = 'jobdest class "{}" not supported'
        msg = msg.format(jobdest_class_name)
        raise(ConfigError(msg))
    jobdest = JobDestClass(jobdest_name)
    return jobdest


def submit_job(jobdest_name, script_path, output_path):
    jobdest = _create_jobdest(jobdest_name)
    jobdest.submit(script_path, output_path)


def print_job(jobdest_name, script_path, output_path):
    jobdest = _create_jobdest(jobdest_name)
    job = jobdest.generate_job(script_path, output_path)
    sys.stdout.write(job)


def kill_job(jobid):
    jobdb = JobInfoSubFileDB()
    jobinfo = jobdb.read(jobid)
    jobdest_name = jobinfo['dest']
    jobdest = _create_jobdest(jobdest_name)
    job = jobdest.kill(jobid)


def stat_job(jobid):
    jobdb = JobInfoSubFileDB()
    jobinfo = jobdb.read(jobid)
    jobdest_name = jobinfo['dest']
    jobdest = _create_jobdest(jobdest_name)
    job = jobdest.stat(jobid)


# ------------------------------
# commandline handling
# ------------------------------


def _set_logging(verbose):
    if verbose == 1:
        level = log.INFO
    elif verbose >= 2:
        level = log.DEBUG
    else:
        level = None
    if verbose > 0:
        format = "%(asctime)s - %(levelname)s - %(message)s"
        log.basicConfig(format=format, level=level)



def _parse_args(parser, argv):
    parser.add_argument('-v',
            dest = 'verbose',
            action = 'count',
            default = 0,
            help = 'verbose (-vv = more verbose)'
            )
    args = parser.parse_args(argv)
    _set_logging(args.verbose)
    return args



def command_submit(prog, argv):
    prog = prog + ' submit'
    parser = argparse.ArgumentParser(
            description = 'submit job',
            prog = prog
            )
    parser.add_argument('jobdest',
            metavar = 'JOBDEST',
            help = 'job destination name'
            )
    parser.add_argument('script_path',
            metavar = 'SCRIPT',
            help = 'script path'
            )
    parser.add_argument('jobout_path',
            metavar = 'JOBOUT',
            help = 'job output path'
            )
    args = _parse_args(parser, argv)
    log.info(' '.join([prog] + argv))
    script_path = os.path.abspath(
            os.path.expanduser(os.path.expandvars(args.script_path)))
    jobout_path = os.path.abspath(
            os.path.expanduser(os.path.expandvars(args.jobout_path)))
    submit_job(args.jobdest, script_path, jobout_path)



def command_print(prog, argv):
    prog = prog + ' print'
    parser = argparse.ArgumentParser(
            description = 'print job',
            prog = prog
            )
    parser.add_argument('jobdest',
            metavar = 'JOBDEST',
            help = 'job destination name'
            )
    parser.add_argument('script_path',
            metavar = 'SCRIPT',
            help = 'script path'
            )
    parser.add_argument('jobout_path',
            metavar = 'JOBOUT',
            help = 'job output path'
            )
    args = _parse_args(parser, argv)
    log.info(' '.join([prog] + argv))
    print_job(args.jobdest, args.script_path, args.jobout_path)



def command_kill(prog, argv):
    prog = prog + ' kill'
    parser = argparse.ArgumentParser(
            description = 'kill job',
            prog = prog
            )
    parser.add_argument('job_info_path',
            metavar = 'JOBINFO',
            help = 'job submission log file'
            )
    args = _parse_args(parser, argv)
    log.info(' '.join([prog] + argv))
    kill_job(args.job_info_path)



def command_status(prog, argv):
    prog = prog + ' status'
    parser = argparse.ArgumentParser(
            description = 'job status',
            prog = prog
            )
    parser.add_argument('job_info_path',
            metavar = 'JOBINFO',
            help = 'job submission log file'
            )
    args = _parse_args(parser, argv)
    log.info(' '.join([prog] + argv))
    stat_job(args.job_info_path)



def main(argv=sys.argv):
    prog = os.path.basename(argv[0])
    command_handlers = OrderedDict((
            ('submit', command_submit),
            ('kill', command_kill),
            ('status', command_status),
            ('print', command_print)
            ))
    parser = argparse.ArgumentParser(
            description = 'Job manager',
            prog = prog
            )
    parser.add_argument('command',
            choices = command_handlers.keys(),
            help = 'job management command'
            )
    parser.add_argument('args',
            metavar = '...',
            nargs = argparse.REMAINDER,
            help = 'command arguments'
            )
    args = parser.parse_args()
    handler = command_handlers[args.command]
    return handler(prog, args.args)


if __name__ == '__main__':
    sys.exit(main())