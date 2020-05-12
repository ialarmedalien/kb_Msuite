from kb_Msuite.Utils.BinnedContigFilter import BinnedContigFilter
from kb_Msuite.Utils.CheckMUtil import CheckMUtil
from kb_Msuite.Utils.DataStagingUtils import DataStagingUtils
from kb_Msuite.Utils.OutputBuilder import OutputBuilder
from kb_Msuite.Utils.WorkspaceHelper import WorkspaceHelper
from CheckMTestBase import CoreCheckMTestClient
import os


class PatchedCheckMUtil(CheckMUtil):

    def __init__(self, config, context, test_args):
        super().__init__(config, context)
        setattr(self, 'test_args', test_args)

    def _exec_subprocess(self, command_args, log_file_args):
        self.logger.info("starting _exec subprocess!")
        test_args = getattr(self, 'test_args')
        unittest = test_args['unittest_testcase']
        tests = test_args['tests']
        tests(unittest, self, command_args, log_file_args)
        self.logger.info("Finishing _exec subprocess!")
        return test_args['return_value']


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

        self.assertTrue(hasattr(cmu, '_exec_subprocess'))

    def test_checkM_core(self):

        with self.subTest('subprocess successful'):

            def tests(self, cmu, command_args, log_file_args):
                run_config = cmu.run_config()
                self.assertEquals(
                    command_args,
                    ['checkm', 'lineage_wf', run_config['input_dir'], run_config['output_dir']]
                )
                self.assertEquals(
                    log_file_args,
                    os.path.join(run_config['logs_dir'], 'lineage_wf.log')
                )

            cmu = PatchedCheckMUtil(self.cfg, self.ctx, {
                'return_value': 0,
                'unittest_testcase': self,
                'tests': tests,
            })
            run_config = cmu.run_config()

            lineage_wf_options = {
                'bin_folder':   run_config['input_dir'],
                'out_folder':   run_config['output_dir'],
                'threads':      cmu.threads,
                'reduced_tree': 1,
            }

            return_value = cmu.run_checkM('lineage_wf', lineage_wf_options)
            self.assertEquals(return_value, 0)

        with self.subTest('subprocess failed'):

            def tests(self, cmu, command_args, log_file_args):
                run_config = cmu.run_config()
                self.assertEquals(
                    command_args,
                    [
                        'checkm', 'lineage_wf', '--reduced_tree',
                        run_config['input_dir'], run_config['output_dir']
                    ]
                )
                self.assertEquals(
                    log_file_args,
                    os.path.join(run_config['logs_dir'], 'lineage_wf.log')
                )

            # add in the reduced_tree option
            cmu = PatchedCheckMUtil(self.cfg, self.ctx, {
                'return_value': 666,
                'unittest_testcase': self,
                'tests': tests,
            })
            run_config = cmu.run_config()
            lineage_wf_options = {
                'bin_folder':   run_config['input_dir'],
                'out_folder':   run_config['output_dir'],
                'threads':      cmu.threads,
                'reduced_tree': 1,
            }

            err_str = 'Stopped execution due to exit code 666'
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.run_checkM('lineage_wf', lineage_wf_options)

    def test_build_checkM_lineage_wf_plots(self):

        def tests(self, cmu, command_args, log_file_args):
            run_config = cmu.run_config()
            if hasattr(cmu, 'tetra_tests_done'):
                # testing the dist plot command
                log_output_file = os.path.join(run_config['logs_dir'], 'dist_plot.log')

                dist_plot_options = {
                    'out_folder':   run_config['output_dir'],
                    'bin_folder':   run_config['input_dir'],
                    'plots_folder': run_config['plots_dir'],
                    'tetra_file':   run_config['tetra_file'],
                    'dist_value':   95,
                    'quiet':        1,
                }

                opts = ['out_folder', 'bin_folder', 'plots_folder', 'tetra_file', 'dist_value']
                command = [
                    'checkm', 'dist_plot', '--quiet'
                ] + [str(dist_plot_options[_]) for _ in opts]
                self.assertEquals(command_args, command)
                self.assertEquals(log_file_args, log_output_file)
                return
            # otherwise, we're testing the tetra command
            log_output_file = os.path.join(run_config['logs_dir'], 'tetra.log')
            command = [
                'checkm', 'tetra', '-t', cmu.threads, '--quiet',
                run_config['all_seq_fasta'],
                run_config['tetra_file'],
            ]
            self.assertEquals(command_args, command)
            self.assertEquals(log_file_args, log_output_file)
            # set an attribute to demonstrate the tetra has been done
            setattr(cmu, 'tetra_tests_done', True)

        cmu = PatchedCheckMUtil(self.cfg, self.ctx, {
            'return_value': 0,
            'unittest_testcase': self,
            'tests': tests,
        })
        cmu.build_checkM_lineage_wf_plots()

        # cmu = CheckMUtil(self.cfg, self.ctx)
        # self.assertTrue(hasattr(CheckMUtil, '_exec_subprocess'))

        # mock_exec.return_value = 0

        # with self.subTest(msg='tetra command'):

        #     log_output_file = os.path.join(run_config['logs_dir'], 'tetra.log')
        #     command = [
        #         'checkm', 'tetra', '-t', cmu.threads, '--quiet',
        #         run_config['all_seq_fasta'],
        #         run_config['tetra_file'],
        #     ]

        #     tetra_options = {
        #         'seq_file':    run_config['all_seq_fasta'],
        #         'tetra_file':  run_config['tetra_file'],
        #         'threads':     self.threads,
        #         'quiet':       1,
        #     }
        #     self.run_checkM('tetra', tetra_options)

        #     mock_exec.assert_called_with(command, log_output_file)
        #     self.assertTrue(os.path.isfile(log_output_file))
        #     shutil.rmtree(run_config['logs_dir'], ignore_errors=True)

        # with self.subTest(msg='dist_plot command'):
        #     # plot distributions for each bin
        #     dist_plot_options = {
        #         'bin_folder':   run_config['input_dir'],
        #         'out_folder':   run_config['output_dir'],
        #         'plots_folder': run_config['plots_dir'],
        #         'tetra_file':   run_config['tetra_file'],
        #         'dist_value':   95,
        #         'quiet':        1,
        #     }

        #     log_output_file = os.path.join(run_config['logs_dir'], 'dist_plot.log')
        #     opts = ['out_folder', 'bin_folder', 'plots_folder', 'tetra_file', 'dist_value']
        #     command = [
        #         'checkm', 'dist_plot', '--quiet'
        #     ] + [str(dist_plot_options[_]) for _ in opts]

        #     self.run_checkM('dist_plot', dist_plot_options)
        #     mock_exec.assert_called_with(command, log_output_file)
        #     self.assertTrue(os.path.isfile(log_output_file))
