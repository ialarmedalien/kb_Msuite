# -*- coding: utf-8 -*-
import os
from pathlib import Path
from random import randint
from uuid import uuid4
from itertools import repeat
import tempfile
from core_checkM_test import CheckMTestBase

from kb_Msuite.Utils.FileUtils import (
    cat_fasta_files,
    clean_up_bin_ID,
    fasta_seq_len_at_least,
    get_fasta_files,
    read_bin_stats_file,
    set_fasta_file_extensions
)


class TestFileUtils(CheckMTestBase):

    def test_01_fileutils_clean_up_bin_id(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_fileutils_clean_up_bin_id")
        self.logger.info("=================================================================\n")

        expected = [
            ['bin.056.fasta', 'bin.056', 'bin.056.fasta'],
            ['out_header_05.fasta', 'out_header_05', 'out_header_05.fasta'],
            ['assembly.fasta', 'assembly', 'assembly.fasta'],
            ['../../this.is.fake', '../../this.is.fake', '../../this.is'],
        ]

        for bid in expected:
            clean_ID = clean_up_bin_ID(bid[0], 'fasta')
            self.assertEqual(clean_ID, bid[1])
            clean_ID = clean_up_bin_ID(bid[0], '.fasta')
            self.assertEqual(clean_ID, bid[1])
            clean_ID = clean_up_bin_ID(bid[0], '.fake')
            self.assertEqual(clean_ID, bid[2])

    def test_01_fileutils_fasta_seq_len_at_least(self):

        assembly_dir = os.path.join('data', 'assemblies')
        empty_assembly_path = os.path.join(assembly_dir, 'empty_assembly.fasta')
        one_nt_assembly_path = os.path.join(assembly_dir, '1_nt_assembly.fasta')
        big_assembly_path = os.path.join(assembly_dir, 'assembly.fasta')

        # default: min length 1
        self.assertFalse(
            fasta_seq_len_at_least(empty_assembly_path)
        )
        self.assertTrue(
            fasta_seq_len_at_least(one_nt_assembly_path)
        )
        self.assertTrue(
            fasta_seq_len_at_least(big_assembly_path)
        )
        # set the min length to 2
        self.assertFalse(
            fasta_seq_len_at_least(empty_assembly_path, 2)
        )
        self.assertFalse(
            fasta_seq_len_at_least(one_nt_assembly_path, 2)
        )
        self.assertTrue(
            fasta_seq_len_at_least(big_assembly_path, 2)
        )

        with self.assertRaisesRegexp(ValueError, 'Minimum length must be 1 or greater'):
            fasta_seq_len_at_least(empty_assembly_path, 0)

    def test_01_fileutils_set_fasta_file_extensions(self):

        with tempfile.TemporaryDirectory() as test_dir:

            extensions = ['.fasta', '.fas', '.fa', '.fsa', '.seq', '.fna', '.ffn', '.faa', '.frn']
            invalid_ext = ['.jpg', '.fasta.tar.gz', '.fa.zip', '.fnaaaar']

            dir_inventory = {}
            n = 0
            for ext in extensions + invalid_ext:
                file_path = os.path.join(test_dir, 'file_00' + str(n) + ext)
                Path(file_path).touch()
                self.assertTrue(os.path.isfile(file_path))
                dir_inventory['file_00' + str(n)] = file_path
                n += 1

            fasta_files_by_clean_bin_ID = get_fasta_files(test_dir, 'fasta')
            self.assertEqual({'file_000': dir_inventory['file_000']}, fasta_files_by_clean_bin_ID)

            # no files with the new extension (yet)
            self.assertEqual({}, get_fasta_files(test_dir, '007'))

            set_fasta_file_extensions(test_dir, '007')
            # expect 1 - 9 to have the extension .007
            new_dir_inventory = {}
            n = 0
            for ext in extensions:
                ext_file = os.path.join(test_dir, 'file_00' + str(n) + '.007')
                self.assertTrue(os.path.isfile(ext_file))
                new_dir_inventory['file_00' + str(n)] = ext_file
                n += 1

            for ext in invalid_ext:
                pos = n - len(extensions)
                self.assertTrue(os.path.isfile(
                    os.path.join(test_dir, 'file_00' + str(n) + invalid_ext[pos]))
                )
                n += 1

            # ensure that we get the expected results from get_fasta_files
            fasta_files_by_clean_bin_ID = get_fasta_files(test_dir, 'fasta')
            self.assertEqual({}, get_fasta_files(test_dir, 'fasta'))
            self.assertEqual(new_dir_inventory, get_fasta_files(test_dir, '007'))

            # check that set_fasta_file_extensions will happily overwrite an extant file
            # TODO: it would be better if this complained, rather than automatically
            # overwriting the file
            new_file = os.path.join(test_dir, 'file_007.faa')
            Path(os.path.join(test_dir, 'file_007.faa')).touch()
            self.assertTrue(os.path.isfile(new_file))
            self.assertTrue(os.path.isfile(os.path.join(test_dir, 'file_007.007')))
            set_fasta_file_extensions(test_dir, '.007')
            self.assertFalse(os.path.exists(new_file))
            self.assertTrue(os.path.isfile(os.path.join(test_dir, 'file_007.007')))

    def test_01_fileutils_read_bin_stats_file(self):

        # non-existent file: return empty dict
        self.assertEqual({}, read_bin_stats_file('/path/to/pretend/file'))

        # this could do with more testing...

    def check_cat_files(self, target_dir, ext, output_file, expected_lines):

        self.assertFalse(os.path.exists(output_file))
        cat_fasta_files(target_dir, ext, output_file)
        self.assertTrue(os.path.isfile(output_file))

        with open(output_file, 'r') as output_fh:
            lines = output_fh.read()

        # split on 'FILE: file_' to create an array of the different file contents
        sep = 'FILE: file_'
        parsed_lines = [sep + _ for _ in lines.split(sep) if _]

        self.assertEqual(set(parsed_lines), set(expected_lines))

    def make_file_contents(self, filename):
        # generate a random number of strings with which to populate the file
        random_text = "".join([str(uuid4()) + '\n' for _ in repeat(None, randint(1, 6))])
        return 'FILE: ' + filename + '\n' + random_text

    def test_01_fileutils_cat_fasta_files(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_fileutils_cat_fasta_files")
        self.logger.info("=================================================================\n")

        with tempfile.TemporaryDirectory() as tempdir:
            # set up a test directory with several files with different extensions
            target_dir = os.path.join(tempdir, 'test_files')
            results_dir = os.path.join(tempdir, 'results')
            os.makedirs(target_dir)
            os.makedirs(results_dir)

            ext_list = ['fasta', 'fa.zip', 'fa', 'fasta.tar.gz', 'fasta', 'fa', 'fa', 'far']
            filenames = ['file_' + str(_) + '.' + ext_list[_] for _ in range(len(ext_list))]
            files = {f: self.make_file_contents(f) for f in filenames}

            for name, contents in files.items():
                file_path = os.path.join(target_dir, name)
                with open(file_path, 'w') as fh:
                    fh.write(contents)

                self.assertTrue(os.path.isfile(file_path))
                self.assertTrue(os.path.getsize(file_path) > 0)

            for ext in ['fasta', 'fa', 'tar']:
                output_file = os.path.join(results_dir, ext + '_concat_file.txt')
                expected_lines = [files[f] for f in files.keys() if f.endswith(ext)]
                self.check_cat_files(target_dir, ext, output_file, expected_lines)
