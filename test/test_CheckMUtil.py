from kb_Msuite.Utils.BinnedContigFilter import BinnedContigFilter
from kb_Msuite.Utils.CheckMUtil import CheckMUtil
from kb_Msuite.Utils.DataStagingUtils import DataStagingUtils
from kb_Msuite.Utils.OutputBuilder import OutputBuilder
from kb_Msuite.Utils.WorkspaceHelper import WorkspaceHelper

from CheckMTestBase import CoreCheckMTestClient

import os
from unittest import mock
import shutil


class TestCheckMUtil(CoreCheckMTestClient):

    def test_00_module_init(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 00_module_init")
        self.logger.info("=================================================================\n")

        self.prep_ref_data()

        cmu = CheckMUtil(self.cfg, self.ctx)
        # run config not yet initialised
        self.assertFalse(hasattr(cmu, '_run_config'))

        run_config = cmu.run_config()
        self.assertIsNotNone(run_config)
        self.assertTrue(hasattr(cmu, '_run_config'))

        # ensure the other attributes are also populated
        obj_name_to_type = {
            'datastagingutils': DataStagingUtils,
            'outputbuilder': OutputBuilder,
            'workspacehelper': WorkspaceHelper,
            'binnedcontigfilter': BinnedContigFilter,
        }
        for attr, type in obj_name_to_type.items():
            self.assertTrue(hasattr(cmu, attr))
            self.assertIsInstance(getattr(cmu, attr), type)

        # ensure we can reset the run_config
        base_dir = run_config['base_dir']
        cmu._set_run_config({'dir_name': 'last_chance_directory'})
        self.assertRegex(cmu.run_config()['base_dir'], r'run___last_chance_directory__\d+')
        self.assertNotEqual(base_dir, cmu.run_config()['base_dir'])

        self.clean_up_cmu(cmu)

    @mock.patch('kb_Msuite.Utils.CheckMUtil._exec_subprocess')
    def test_build_checkM_lineage_wf_plots(self, mock_exec):

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        # compute tetranucleotide frequencies based on the concatenated fasta file

        with self.subTest(msg='tetra command'):

            log_output_file = os.path.join(run_config['logs_dir'], 'tetra.log')
            command = [
                'checkm', 'tetra', '-t', cmu.threads, '--quiet',
                run_config['all_seq_fasta'],
                run_config['tetra_file'],
            ]
            mock_exec.return_value = 0

            tetra_options = {
                'seq_file':    run_config['all_seq_fasta'],
                'tetra_file':  run_config['tetra_file'],
                'threads':     self.threads,
                'quiet':       1,
            }
            self.run_checkM('tetra', tetra_options)

            mock_exec.assert_called_with(command, log_output_file)
            self.assertTrue(os.path.isfile(log_output_file))
            shutil.rmtree(run_config['logs_dir'], ignore_errors=True)

        with self.subTest(msg='dist_plot command'):
            # plot distributions for each bin
            dist_plot_options = {
                'bin_folder':   run_config['input_dir'],
                'out_folder':   run_config['output_dir'],
                'plots_folder': run_config['plots_dir'],
                'tetra_file':   run_config['tetra_file'],
                'dist_value':   95,
                'quiet':        1,
            }

            log_output_file = os.path.join(run_config['logs_dir'], 'dist_plot.log')
            opts = ['out_folder', 'bin_folder', 'plots_folder', 'tetra_file', 'dist_value']
            command = [
                'checkm', 'dist_plot', '--quiet'
            ] + [str(dist_plot_options[_]) for _ in opts]

            self.run_checkM('dist_plot', dist_plot_options)
            mock_exec.assert_called_with(command, log_output_file)
            self.assertTrue(os.path.isfile(log_output_file))

    @mock.patch('kb_Msuite.Utils.CheckMUtil._exec_subprocess')
    def test_checkM_core(self, mock_exec):

        cmu = self.checkMUtil
        run_config = cmu.run_config()

        lineage_wf_options = {
            'bin_folder':   run_config['input_dir'],
            'out_folder':   run_config['output_dir'],
            'threads':      cmu.threads,
            'reduced_tree': 1,
        }

        command = [
            'checkm', 'lineage_wf', run_config['input_dir'], run_config['output_dir']
        ]
        log_output_file = os.path.join(run_config['logs_dir'], 'lineage_wf.log')

        with self.subTest('subprocess successful'):

            mock_exec.return_value = 0
            cmu.run_checkM('lineage_wf', lineage_wf_options)

            mock_exec.assert_called_with(command, log_output_file)
            self.assertTrue(os.path.isfile(log_output_file))
            shutil.rmtree(run_config['logs_dir'], ignore_errors=True)

        # add in the reduced_tree option
        lineage_wf_options['reduced_tree'] = 1
        command.insert(2, '--reduced_tree')

        with self.subTest('subprocess failed'):
            mock_exec.return_value = 666
            err_str = 'Stopped execution due to exit code 666'

            with self.assertRaisesRegex(ValueError, err_str):
                cmu.run_checkM('lineage_wf', lineage_wf_options)

            mock_exec.assert_called_with(command, log_output_file)
            self.assertTrue(os.path.isfile(log_output_file))
