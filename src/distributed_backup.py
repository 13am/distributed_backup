import sys
import argparse
import os
import datetime
import subprocess
from collections import defaultdict


if sys.version_info[0] < 3:
    sys.exit('This program requires Python 3.0 or newer')


DEBUG = False
FILES_SUBFOLDER_NAME = 'files'
JOBS_SUBFOLDER_NAME = '.distributed_backup_jobs'
JOBS_TODO_SUBFOLDER_NAME = os.sep.join((JOBS_SUBFOLDER_NAME, 'todo'))
JOBS_DONE_SUBFOLDER_NAME = os.sep.join((JOBS_SUBFOLDER_NAME, 'done'))
CATALOG_FNAME = 'catalog.txt'
LOCFILE_EXTENSION = '.loc'
COMPRESSED_EXTENSION = '.tar.gz'


def do_print(s, same_line=False):
    newline_prefix = '# '
    s = str(s).rstrip('\n')
    s = s.replace('\n', '\n' + newline_prefix)
    print(newline_prefix + str(s))


def parse_options():

    parser = argparse.ArgumentParser()

    parser.add_argument('--source',
                        type=str,
                        action='store',
                        default=None,
                        dest='source')

    parser.add_argument('--destination',
                        type=str,
                        action='store',
                        default=None,
                        dest='destination')

    parser.add_argument('--restore',
                        action='store_true',
                        default=False,
                        dest='restore')

    parser.add_argument('--backup',
                        action='store_true',
                        default=False,
                        dest='backup')

    parser.add_argument('--verify-backup',
                        action='store_true',
                        default=False,
                        dest='verify_backup')

    parser.add_argument('--verify-restore',
                        action='store_true',
                        default=False,
                        dest='verify_restore')

    parser.add_argument('--check-backup-todo',
                        action='store_true',
                        default=False,
                        dest='check_backup_todo')

    parser.add_argument('--check-restore-todo',
                        action='store_true',
                        default=False,
                        dest='check_restore_todo')

    parser.add_argument('--include-script',
                        action='store_true',
                        default=False,
                        dest='include_script')

    parser.add_argument('--yes',
                        action='store_true',
                        default=False,
                        dest='yes')

    parser.add_argument('--no-interactive',
                        action='store_true',
                        default=False,
                        dest='no_interactive')

    parser.add_argument('--verbose',
                        action='store_true',
                        default=False,
                        dest='verbose')

    options = parser.parse_args()

    # make the --source and --destination paths absolute
    if options.source is not None:
        options.source = os.path.abspath(options.source)
    if options.destination is not None:
        options.destination = os.path.abspath(options.destination)

    if DEBUG:
        options.verbose = True

    return options


def _check_dir_existence(description=None, fpath=None, expected=True):
    msg = None
    if msg is None and fpath is None:
        msg = 'Please define --{}'.format(description)
    if msg is None and os.path.exists(fpath) is not expected:
        if expected:
            verb = 'does not exist'
        else:
            verb = 'already exists'
        msg = 'The --{} directory "{}" {}'
        msg = msg.format(description, fpath, verb)
    if expected and msg is None and os.path.isdir(fpath) is False:
        msg = 'The --{} parameter "{}" is not a directory'
        msg = msg.format(description, fpath)
    if msg is not None:
        exit_error(msg)


def check_dir_existence(options, which, expected):
    if which == 'source':
        _check_dir_existence(description=which,
                             fpath=options.source,
                             expected=expected)
    elif which == 'destination':
        _check_dir_existence(description=which,
                             fpath=options.destination,
                             expected=expected)
    else:
        raise RuntimeError


def check_source_and_destination(options):
    check_dir_existence(options, 'source', True)
    check_dir_existence(options, 'destination', False)
    return True


def exit_error(msg):
    do_print('Error: ' + msg.replace('\n', '\n# '))
    sys.exit(1)


# make the base file name for all files related to a specific directory
def get_dir_fname(pth):
    p = str(pth)
    while p.startswith('/') or p.startswith('.'):
        p = p.lstrip('/')
        p = p.lstrip('.')
    while '__' in p:
        p = p.replace('__', '_')
    p = p.replace('/', '__')
    return p


def get_dir_description(pth):
    if os.path.isdir(pth) is False:
        raise ValueError
    vals = []
    vals.append('PATH\t' + pth)
    vals.append('ARCHIVE_TIME\t' + str(datetime.datetime.utcnow()))
    contents = sorted(os.listdir(pth))
    contents = {i: os.sep.join((pth, i)) for i in contents}
    for i in sorted(contents.keys()):
        if os.path.isdir(contents[i]):
            vals.append('DIRECTORY\t{}'.format(i))
    for i in sorted(contents.keys()):
        if not os.path.isdir(contents[i]):
            vals.append('FILE\t{}\t{}'.format(i, contents[i]))
    return '\n'.join(vals)


def make_backup_script(loc_fpath=None):
    ipdir = None
    with open(loc_fpath, 'r') as ip:
        for line in ip:
            line = line.split()
            if line[0] == 'PATH':
                ipdir = line[1]
                break
    if ipdir is None:
        exit_error('PATH not found in {}'.format(loc_fpath))
    opname = loc_fpath[:-len(LOCFILE_EXTENSION)] + COMPRESSED_EXTENSION
    awk_fnames = ('awk \'BEGIN {{FS="\\t"}}; '
                  '$1 == "FILE" && NF == 3'
                  '{{print $2}}\' {}'.format(loc_fpath))
    tar_cmd = 'tar -czv --files-from=- -f {}'.format(opname)
    # note: the "" on the next line is required to get the two
    # spaces required by md5sum spec between the sum and the file name
    md5_cmd = 'echo `md5sum {} | cut -d \' \' -f 1` "" {} > {}.md5'
    md5_cmd = md5_cmd.format(opname,
                             opname.split(os.sep)[-1],
                             opname)
    op = ['#!/bin/bash',
          'cd {}'.format(ipdir),
          awk_fnames + ' | ' + tar_cmd,
          md5_cmd]
    return '\n'.join(op) + '\n'


def make_restore_script(fpath=None, destination_dir=None):
    tar_cmd = 'tar -xvf {} -C .'.format(fpath)
    op = ['#!/bin/bash',
          'cd {}'.format(destination_dir),
          tar_cmd]
    return '\n'.join(op) + '\n'


def md5file(fpath):
    fname = os.path.basename(fpath)
    proc = subprocess.Popen(['md5sum', fpath],
                            stdout=subprocess.PIPE)
    out, err = proc.communicate()
    if err is not None:
        raise RuntimeError
    out = out.decode('utf-8')
    md5sum = out.strip().split()[0]
    with open(fpath + '.md5', 'w') as op:
        op.write('{}  {}\n'.format(md5sum, fname))  # TWO SPACES REQUIRED!
    return True


def md5check(fpath):
    fdir = os.path.dirname(fpath)
    output_stdout = subprocess.DEVNULL
    output_stderr = subprocess.DEVNULL
    if DEBUG:
        output_stdout = subprocess.stdout
        output_stderr = subprocess.stderr
    ok = subprocess.call(['md5sum', '-c', fpath + '.md5'],
                         stdout=output_stdout,
                         stderr=output_stderr,
                         cwd=fdir)
    return ok == 0


def ask_to_run_job_scripts_locally(options):
    action = None
    if options.restore:
        action = 'restoring'
    if options.backup:
        action = 'backing up'

    script_folder_todo = os.sep.join((options.destination,
                                     JOBS_TODO_SUBFOLDER_NAME))
    script_folder_done = os.sep.join((options.destination,
                                     JOBS_DONE_SUBFOLDER_NAME))

    msg = ('\nThe job scripts for {} the files are now located in \n"{}". '
           '\nIf you want to execute the scripts immediately on this machine,'
           '\ntype "yes" and press Enter. Otherwise, just press Enter.'
           '\n> ')

    response = 'no'
    if options.yes:
        response = 'yes'
    else:
        response = input(msg.format(action, script_folder_todo))
    if response != 'yes':
        return response

    msg = 'Executing the job scripts from "{}" locally.'
    do_print(msg.format(script_folder_todo))

    counter = 0
    fails = 0
    for fname in os.listdir(script_folder_todo):
        if fname.endswith('.sh'):
            fpath_todo = os.sep.join((script_folder_todo, fname))
            fpath_done = os.sep.join((script_folder_done, fname))
            args = ['bash', fpath_todo]
            send_op_to = subprocess.DEVNULL

            if options.verbose:
                print('run "{}"'.format(' '.join(args)))
                send_op_to = sys.stderr

            ok = subprocess.call(['bash', fpath_todo],
                                 stdout=send_op_to,
                                 stderr=send_op_to)
            if ok == 0:
                subprocess.call(['mv', fpath_todo, fpath_done],
                                stdout=send_op_to,
                                stderr=send_op_to)
                counter += 1
            else:
                fails += 1

    if fails == 0:
        msg = 'All {} scripts executed successfully.'
        args = [counter]
    else:
        msg = ('WARNING: {} scripts failed to execute, these'
               ' are found in \n"{}"\n. {} scripts executed succesfully'
               ' and are now located in \n{}')
        args = (fails, script_folder_todo, counter, script_folder_done)
    do_print(msg.format(*args))

    return response


def prepare_backups(options):

    check_source_and_destination(options)
    do_print('Preparing to back up data.')

    # make the necessary subfolders and the catalog file
    os.mkdir(options.destination)
    subdirs = (FILES_SUBFOLDER_NAME,
               JOBS_SUBFOLDER_NAME,
               JOBS_DONE_SUBFOLDER_NAME,
               JOBS_TODO_SUBFOLDER_NAME)
    for i in subdirs:
        os.mkdir(os.sep.join((options.destination, i)))

    catalog_fpath = os.sep.join((options.destination, CATALOG_FNAME))
    with open(catalog_fpath, 'w') as op:
        op.write('# START\n')
        op.write('# SOURCE\t{}\n'.format(options.source))
        for dirpath, dirnames, fnames in os.walk(options.source):
            loc_fname = get_dir_fname(dirpath) + LOCFILE_EXTENSION
            op.write(loc_fname + '\n')
        op.write('# END\n')
    md5file(catalog_fpath)

    # write the .loc files and the job scripts to compress the data
    counter = 0
    for dirpath, dirnames, fnames in os.walk(options.source):
        do_print(dirpath, same_line=True)
        loc_fname = get_dir_fname(dirpath) + LOCFILE_EXTENSION
        loc_fpath = os.sep.join((options.destination,
                                 FILES_SUBFOLDER_NAME,
                                 loc_fname))
        with open(loc_fpath, 'w') as op:
            description = get_dir_description(dirpath)
            op.write(description)
        md5file(loc_fpath)

        script_fname = get_dir_fname(dirpath) + '.sh'
        script_fpath = os.sep.join((options.destination,
                                    JOBS_TODO_SUBFOLDER_NAME,
                                    script_fname))
        with open(script_fpath, 'w') as op:
            script = make_backup_script(loc_fpath=loc_fpath)
            op.write(script)
        counter += 1
    msg = '{} directories prepared for backup'.format(counter)
    do_print(msg)

    # make a copy of this script to the destination folder
    if options.include_script:
        backup_script_fpath = os.path.realpath(__file__)
        backup_script_fname = os.path.basename(backup_script_fpath)
        backup_script_copy_path = os.sep.join((options.destination,
                                               backup_script_fname))
        subprocess.call(['cp',
                         backup_script_fpath,
                         backup_script_copy_path])
        md5file(backup_script_copy_path)


def verify_catalog(options):
    do_print('Verifying that the catalog file is intact and present.')
    catalog_fpath = os.sep.join((options.destination, CATALOG_FNAME))

    if os.path.exists(catalog_fpath) is False:
        what_missing = 'catalog file'
        what_name = catalog_fpath
        if os.path.exists(options.destination) is False:
            what_missing = '--destination folder'
            what_name = options.destination
        msg = ('the {} "{}" is missing.'
               '\nVerification: FAILURE')
        exit_error(msg.format(what_missing, what_name))

    start_ok = False
    end_ok = False
    source_ok = False
    with open(catalog_fpath, 'r') as ip:
        for line in ip:
            line = line.strip()
            if line.startswith('#'):
                if line == '# START':
                    start_ok = True
                if line == '# END':
                    end_ok = True
                if line.split('\t')[0] == '# SOURCE':
                    source_ok = True

    if not start_ok or not end_ok:
        msg = ('the catalog file {} is missing the start or end tag.'
               '\nVerification: FAILURE')
        exit_error(msg.format(catalog_fpath))

    if not source_ok:
        msg = ('the catalog file {} is missing the SOURCE tag.'
               '\nVerification: FAILURE')
        exit_error(msg.format(catalog_fpath))


def verify_backups(options):

    verify_catalog(options)
    do_print('Verifying that the backed up files exist and are intact.')

    # verify all md5sums
    md5sums = []
    catalog_fpath = os.sep.join((options.destination, CATALOG_FNAME))
    with open(catalog_fpath, 'r') as ip:
        for line in ip:
            line = line.strip()
            if not line.startswith('#'):
                loc_fpath = os.sep.join((options.destination,
                                         FILES_SUBFOLDER_NAME,
                                         line))
                tar_fname = line[:-len(LOCFILE_EXTENSION)]
                tar_fname = tar_fname + COMPRESSED_EXTENSION
                tar_fpath = os.sep.join((options.destination,
                                         FILES_SUBFOLDER_NAME,
                                         tar_fname))
                md5sums.append(loc_fpath)
                md5sums.append(tar_fpath)

    fails = []
    for i in md5sums:
        ok = md5check(i)
        if not ok:
            fails.append(i)
    if len(fails) > 0:
        for i in fails:
            msg = 'md5sum fail for {}'.format(i)
            do_print(msg)
        msg = '{} md5sum fails.\nBackup verification: FAILURE'
        exit_error(msg.format(len(fails)))
    else:
        msg = 'All {} md5sums (2 per compressed directory) matched.'
        do_print(msg.format(len(md5sums)))

    do_print('Backup verification: SUCCESS')
    return 0


# returns {full_path_to_loc: full_path_to_loc_original_source}
def list_loc_files(catalog_fpath):
    root_dir = None
    loc_files = []
    with open(catalog_fpath, 'r') as ip:
        for line in ip:
            line = line.strip()
            if line.startswith('# SOURCE'):
                root_dir = line.split('\t')[1]
            if line.startswith('#') is False:
                loc_files.append(line.strip())
    if root_dir is None:
        do_print('SOURCE not found in {} file'.format(catalog_fpath))
        raise RuntimeError

    loc_dir = os.sep.join((os.path.dirname(catalog_fpath),
                           FILES_SUBFOLDER_NAME))
    loc_file_dict = {}
    for i in loc_files:
        fpath = os.sep.join((loc_dir, i))
        with open(fpath, 'r') as ip:
            ok = False
            for line in ip:
                line = line.strip().split('\t')
                if line[0] == 'PATH':
                    loc_file_dict[fpath] = line[1]
                    ok = True
                    break
            if not ok:
                exit_error('PATH not found in {}'.format(fpath))

    return loc_file_dict


def verify_locfile_backup(loc_fpath):
    tar_fpath = loc_fpath[:-len(LOCFILE_EXTENSION)] + COMPRESSED_EXTENSION
    fails = []
    for i in (loc_fpath, tar_fpath):
        ok = md5check(i)
        if not ok:
            fails.append(i)
    return fails


def verify_locfile_restore(options, loc_fpath=None):
    catalog_fpath = os.sep.join((options.source, CATALOG_FNAME))
    root_dir = get_root_dir(catalog_fpath)
    files_and_dirs = []
    with open(loc_fpath, 'r') as ip:

        entries = []
        source_dir = None
        for line in ip:
            line = line.strip().split('\t')
            if line[0] == 'PATH':
                source_dir = line[1]
            if line[0] in ('FILE', 'DIRECTORY'):
                entries.append(line[1])

        if source_dir is None:
            msg = 'PATH not found in {}'.format(loc_fpath)
            msg = msg + '\nRestore verification: FAILURE\n'
            exit_error(msg)
        source_dir = source_dir.split(root_dir, 1)[-1]

        for e in entries:
            e_path = os.sep.join((options.destination, source_dir, e))
            double_sep = os.sep + os.sep
            while double_sep in e_path:
                e_path = e_path.replace(double_sep, os.sep)
            files_and_dirs.append(e_path)

    missings = []
    for i in files_and_dirs:
        if os.path.exists(i) is False:
            missings.append(i)

    return missings


def get_root_dir(catalog_fpath):
    root_dir = None
    with open(catalog_fpath, 'r') as ip:
        for line in ip:
            line = line.strip()
            if line.startswith('# SOURCE'):
                root_dir = line.split('\t')[1]
                break
    return root_dir


def check_todo(options):

    def check_backup_ok(options, loc_fpath=None):
        return verify_locfile_backup(loc_fpath)

    def check_restore_ok(options, loc_fpath=None):
        return verify_locfile_restore(options, loc_fpath=loc_fpath)

    check_dir_existence(options, 'destination', True)

    if options.check_backup_todo:
        list_fun = check_backup_ok
        refdir_catalog = options.destination

    if options.check_restore_todo:
        check_dir_existence(options, 'source', True)
        list_fun = check_restore_ok
        refdir_catalog = options.source

    catalog_fpath = os.sep.join((refdir_catalog,
                                 CATALOG_FNAME))
    script_dir_todo = os.sep.join((options.destination,
                                   JOBS_TODO_SUBFOLDER_NAME))
    script_dir_done = os.sep.join((options.destination,
                                   JOBS_DONE_SUBFOLDER_NAME))

    counter = 0
    loc_files = list_loc_files(catalog_fpath)
    for fpath in loc_files:
        if options.verbose:
            do_print('Check ' + fpath)
        fname = os.path.basename(fpath)
        script_fname = fname[:-len(LOCFILE_EXTENSION)] + '.sh'
        script_fpath_todo = os.sep.join((script_dir_todo,
                                         script_fname))
        script_fpath_done = os.sep.join((script_dir_done,
                                         script_fname))
        if os.path.exists(script_fpath_done):
            continue

        n_files_not_ok = len(list_fun(options, loc_fpath=fpath))
        if n_files_not_ok == 0:
            ok = subprocess.call(['mv',
                                  script_fpath_todo,
                                  script_fpath_done])
            if ok == 0:
                counter += 1

    msg = 'Moved {} scripts from \n"{}" to \n"{}" '
    msg = msg.format(counter, script_dir_todo, script_dir_done)
    do_print(msg)

    counter = 0
    for fname in os.listdir(script_dir_todo):
        if fname.endswith('.sh'):
            counter += 1
    msg = '{} scripts left in \n"{}"'
    msg = msg.format(counter, script_dir_todo)
    do_print(msg)


# return relative paths to the required directories
def get_dir_tree(catalog_fpath):

    # first read the .loc file names
    root_dir = get_root_dir(catalog_fpath)
    if root_dir is None:
        do_print('SOURCE not found in {} file'.format(catalog_fpath))
        raise RuntimeError
    msg = 'Read the original --source directory name "{}" from "{}" '
    if DEBUG:
        do_print(msg.format(root_dir, catalog_fpath))
    loc_files = list_loc_files(catalog_fpath)

    # read all the .loc files to get the directory structure
    dirs = {}
    for fpath, source_path in loc_files.items():
        with open(fpath, 'r') as ip:
            i_dirs = []
            for line in ip:
                try:
                    line = line.strip().split('\t')
                    if line[0] == 'DIRECTORY':
                        i_dirs.append(line[1])
                except:
                    pass
            for dp in i_dirs:
                rel_path = os.sep.join((source_path, dp))
                rel_path = rel_path.split(root_dir, 1)[-1]
                dirs[rel_path] = 0

    if DEBUG:
        for i in dirs:
            do_print(i)

    # reproduce the structure as a defaultdict
    def get_nested_defaultdict():
        return defaultdict(get_nested_defaultdict)
    dir_tree = defaultdict(get_nested_defaultdict)

    for dir_path in dirs:
        rel_path = dir_path.split(root_dir, 1)[-1].split(os.sep)[1:]
        parent = dir_tree
        while len(rel_path) > 0:
            parent = parent[rel_path.pop(0)]

    return dir_tree


def restore_from_backup(options):

    check_source_and_destination(options)
    do_print('Preparing to restore backed up data.')

    # describe the directory structure
    catalog_fpath = os.sep.join((options.source, CATALOG_FNAME))
    dir_tree = get_dir_tree(catalog_fpath)

    # make the necessary main folders
    try:
        os.mkdir(options.destination)
    except:
        msg = 'the --destination "{}" could not be created'
        exit_error(msg.format(options.destination))
    for i in (JOBS_SUBFOLDER_NAME,
              JOBS_DONE_SUBFOLDER_NAME, JOBS_TODO_SUBFOLDER_NAME):
        os.mkdir(os.sep.join((options.destination, i)))

    # create the directory structure
    def iter_and_create(d=None, root_dir=None):
        for i in d:
            dir_path = os.sep.join((root_dir, i))
            try:
                os.mkdir(dir_path)
            except:
                msg = 'the directory "{}" could not be created'
                exit_error(msg.format(i))
            iter_and_create(d=d[i], root_dir=dir_path)
    iter_and_create(d=dir_tree, root_dir=options.destination)
    do_print('Created the directory tree.')

    # make the restore script files
    loc_files = list_loc_files(catalog_fpath)
    root_dir = get_root_dir(catalog_fpath)
    counter = 0
    for loc_fpath, source_path in loc_files.items():
        loc_fname = loc_fpath.split(os.sep)[-1]
        tar_fpath = loc_fpath[:-len(LOCFILE_EXTENSION)] + COMPRESSED_EXTENSION
        destination_relative_dir = source_path.split(root_dir, 1)[-1]
        destination_dir = os.sep.join((options.destination,
                                       destination_relative_dir))
        double_sep = os.sep + os.sep
        while double_sep in destination_dir:
            destination_dir = destination_dir.replace(double_sep,
                                                      os.sep)

        restore_script = make_restore_script(fpath=tar_fpath,
                                             destination_dir=destination_dir)
        if options.verbose:
            do_print('restore "{}" to "{}"'.format(tar_fpath,
                                                   destination_dir))
            do_print(restore_script + '\n')

        script_fname = loc_fname[:-len(LOCFILE_EXTENSION)] + '.sh'
        script_fpath = os.sep.join((options.destination,
                                    JOBS_TODO_SUBFOLDER_NAME,
                                    script_fname))
        with open(script_fpath, 'w') as op:
            op.write(restore_script)
        counter += 1
    do_print('Wrote the scripts to extract {} directories.'.format(counter))
    do_print('Preparing to restore backed up data done.')


def verify_restore(options):
    check_dir_existence(options, 'source', True)
    check_dir_existence(options, 'destination', True)

    # list all files that need to be found
    catalog_fpath = os.sep.join((options.source, CATALOG_FNAME))
    root_dir = get_root_dir(catalog_fpath)
    if root_dir is None:
        msg = 'PATH not found in {}'.format(catalog_fpath)
        msg = msg + '\nRestore verification: FAILURE\n'
        exit_error(msg)

    loc_files = list_loc_files(catalog_fpath)
    files_and_dirs = []
    for fpath in loc_files:
        with open(fpath, 'r') as ip:

            entries = []
            source_dir = None
            for line in ip:
                line = line.strip().split('\t')
                if line[0] == 'PATH':
                    source_dir = line[1]
                if line[0] in ('FILE', 'DIRECTORY'):
                    entries.append(line[1])

            if source_dir is None:
                msg = 'PATH not found in {}'.format(fpath)
                msg = msg + '\nRestore verification: FAILURE\n'
                exit_error(msg)
            source_dir = source_dir.split(root_dir, 1)[-1]

            for e in entries:
                e_path = os.sep.join((options.destination, source_dir, e))
                double_sep = os.sep + os.sep
                while double_sep in e_path:
                    e_path = e_path.replace(double_sep, os.sep)
                files_and_dirs.append(e_path)

    missings = []
    for i in files_and_dirs:
        if os.path.exists(i) is False:
            missings.append(i)
    if len(missings) > 0:
        msg = ('{} files were missing. If you want to output the names'
               ' of the missing files to a file, please type a name for'
               ' the file and press Enter. Otherwise, just press Enter '
               ' to report the names of the missing files here on screen.'
               ' \n> ')
        report_name = input(msg.format(len(missings)))
        if report_name == '':
            op = sys.stdout
        else:
            op = open(report_name, 'w')
        for i in missings:
            op.write(i + '\n')
        if report_name != '':
            op.close()
        do_print('Restore verification: FAILURE')
        sys.exit(1)

    msg = ('Verified that all {} files and folders listed in the .loc files '
           'read from \n"{}"\n'
           'were found in the destination directory "{}"')
    do_print(msg.format(len(files_and_dirs),
                        catalog_fpath,
                        options.destination))
    do_print('Restore verification: SUCCESS')
    return 0


def main():

    options = parse_options()

    if options.check_backup_todo or options.check_restore_todo:
        check_todo(options)

    elif options.verify_backup:
        verify_backups(options)

    elif options.verify_restore:
        verify_restore(options)

    elif options.backup:
        prepare_backups(options)
        if options.no_interactive:
            response = 'no'
        else:
            response = ask_to_run_job_scripts_locally(options)
        if response == 'yes':
            verify_backups(options)

    elif options.restore:
        restore_from_backup(options)
        if options.no_interactive:
            response = 'no'
        else:
            response = ask_to_run_job_scripts_locally(options)
        if response == 'yes':
            verify_restore(options)


if __name__ == '__main__':
    main()
