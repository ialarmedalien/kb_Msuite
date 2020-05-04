import ast
import glob
import logging
import os
import shutil
import subprocess


def _check_extension(extension):
    # ensure that the extension does not start with '.'
    if extension[0] == '.':
        return extension[1:]

    return extension


def _logger(priority, message):

    logger = logging.getLogger('kb_Msuite.FileUtils')
    priority_to_val = {
        'critical': 50,
        'error': 40,
        'warning': 30,
        'warn': 30,
        'info': 20,
        'debug': 10
    }
    priority_numeric = priority_to_val[priority] if priority in priority_to_val else 0
    logger.log(priority_numeric, message)


def clean_up_bin_ID(bin_id, extension=None):
    '''
    remove the specified extension from a string
    '''
    if extension and len(extension) > 0:
        extension = _check_extension(extension)
        bin_id = bin_id.replace('.' + extension, '')

    return bin_id

    # return re.sub(r'^[^.]+.', '', bin_id)


def get_fasta_files(search_dir, extension):
    '''
    find all files with extension `extension` in the directory `search_dir`
    '''

    fasta_ext = _check_extension(extension)
    fasta_files = dict()
    for (dirpath, dirnames, filenames) in os.walk(search_dir):
        for filename in filenames:
            if not os.path.isfile(os.path.join(search_dir, filename)):
                continue
            if filename.endswith('.' + fasta_ext):
                bin_ID = clean_up_bin_ID(filename, fasta_ext)
                fasta_files[bin_ID] = os.path.join(search_dir, filename)
                # _logger('debug', "ACCEPTED: "+bin_ID+" FILE:"+filename)  # DEBUG

    return fasta_files


def fasta_seq_len_at_least(fasta_path, min_fasta_len=1):
    '''
    counts the number of non-header, non-whitespace characters in a FASTA file
    '''
    seq_len = 0
    with open(fasta_path, 'r') as fasta_handle:
        for line in fasta_handle:
            line = line.strip()
            if line.startswith('>'):
                continue
            line = line.replace(' ', '')
            seq_len += len(line)
            if seq_len >= min_fasta_len:
                return True

    _logger(
        'warning',
        {
            'min_length': min_fasta_len,
            'seq_len':    seq_len
        }
    )
    return False


def set_fasta_file_extensions(folder, extension):
    '''
    Renames all detected fasta files in folder to the specified extension.
    fasta files are detected based on its existing extension, which must be one of:
        ['.fasta', '.fas', '.fa', '.fsa', '.seq', '.fna', '.ffn', '.faa', '.frn']

    Note that this WILL overwrite existing files if a file already exists with the same name
    '''
    new_extension = _check_extension(extension)
    extensions = ['.fasta', '.fas', '.fa', '.fsa', '.seq', '.fna', '.ffn', '.faa', '.frn']

    for file in os.listdir(folder):
        if not os.path.isfile(os.path.join(folder, file)):
            continue
        filename, file_extension = os.path.splitext(file)
        if file_extension in extensions:
            # if os.path.exists(os.path.join(folder, filename + '.' + new_extension)):
            os.rename(
                os.path.join(folder, file),
                os.path.join(folder, filename + '.' + new_extension)
            )


def cat_fasta_files(folder, fasta_ext, output_fasta_file, cwd):
    '''
    Given a folder of fasta files with the specified extension, cat them together
    using 'cat' into the target new_fasta_file
    '''
    extension = _check_extension(fasta_ext)
    files = glob.glob(os.path.join(folder, '*.' + extension))
    cat_cmd = ['cat'] + files
    fasta_fh = open(output_fasta_file, 'w')
    p = subprocess.Popen(cat_cmd, cwd=cwd, stdout=fasta_fh, shell=False)
    exitCode = p.wait()
    fasta_fh.close()

    if exitCode != 0:
        raise ValueError(
            'Error running command: ' + ' '.join(cat_cmd) + '\n' +
            'Exit Code: ' + str(exitCode)
        )


def copy_file_ignore_errors(filename, src_folder, dest_folder):

    src = os.path.join(src_folder, filename)
    dest = os.path.join(dest_folder, filename)
    os.makedirs(dest_folder, exist_ok=True)

    return copy_file_new_name_ignore_errors(src, dest)


def copy_file_new_name_ignore_errors(source_path, destination_path):
    # self.logger.debug('copying ' + source_path + ' to ' + destination_path)
    try:
        shutil.copy(source_path, destination_path)
    except Exception as e:
        # TODO: add error message reporting
        _logger('error', {
            'source': source_path,
            'destination': destination_path,
            'error': e,
        })
        raise e


def read_bin_stats_file(stats_file):

    bin_stats = dict()

    if not os.path.isfile(stats_file):
        _logger('warning', 'No stats file found (looking at: ' + stats_file + ')')
        return bin_stats

    with open(stats_file) as stats_fh:
        for line in stats_fh:
            if not line:
                continue
            if line.startswith('#'):
                continue
            col = line.split('\t')
            bid = str(col[0])
            data = ast.literal_eval(col[1])
            bin_stats[bid] = data

    return bin_stats
