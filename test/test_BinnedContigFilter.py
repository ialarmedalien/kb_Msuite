import os
import shutil
from pathlib import Path


from kb_Msuite.Utils.Utils import TSVMixin
from CheckMTestBase import CoreCheckMTestClient


class TestBinnedContigFilter(CoreCheckMTestClient, TSVMixin):

    def setUp(self):
        super().setUp()
        self.require_data('binned_contigs_ref')

        cmu = self.checkMUtil
        run_config = cmu.run_config()

        # copy over a results file
        # 001  'Completeness': 97.6,              'Contamination': 1.907,
        # 002  'Completeness': 98.11542991755006, 'Contamination': 1.4134275618374559,
        # 003  'Completeness': 96.34019795657727, 'Contamination': 1.7600574712643677,
        output_dir = run_config['output_dir']
        os.makedirs(os.path.join(output_dir, 'storage'), exist_ok=True)
        shutil.copy(
            os.path.join('data', 'results', 'example_bin_stats_ext.tsv'),
            run_config['bin_stats_ext_file']
        )

        os.makedirs(run_config['input_dir'], exist_ok=True)
        for bid in [1, 2, 3]:
            bid_path = os.path.join(
                run_config['input_dir'],
                'bin.00' + str(bid) + '.' + run_config['fasta_ext']
            )
            Path(bid_path).touch(exist_ok=True)

    def test_03_filter_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_dodgy_ref', 'report_ref')

        cmu = self.checkMUtil
        cmu.run_config()

        # wrong type
        self.assertIsNone(
            cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.report_ref})
        )
        self.assertIsNone(
            cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.assembly_dodgy_ref})
        )

        # no output_filtered_binnedcontigs_obj_name
        self.assertIsNone(
            cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.binned_contigs_ref})
        )

        # empty input dir
        os.makedirs(cmu.run_config()['input_dir'], exist_ok=True)
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Alpha',
        }))

    def test_03_filter_binned_contigs_checkM_missing_IDs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_checkM_missing_IDs")
        self.logger.info("=================================================================\n")

        cmu = self.checkMUtil
        run_config = cmu.run_config()

        missing_ids = ['bin.000', 'bin.004']
        for bid in missing_ids:
            bid_path = os.path.join(
                run_config['input_dir'], bid + '.' + run_config['fasta_ext']
            )
            Path(bid_path).touch(exist_ok=True)

        err_str = "The following bin IDs are missing from the checkM output: " + \
            ", ".join(missing_ids)
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.binnedcontigfilter.filter_binned_contigs({
                'input_ref': self.binned_contigs_ref,
                'output_filtered_binnedcontigs_obj_name': 'Beta',
            })

    def test_03_filter_binned_contigs_no_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_no_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        # no high quality bins
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Gamma',
            'completeness_perc': 99.0,
            'contamination_perc': 1.0,
            'workspace_name': self.wsName,
        }))
        # no summary file
        self.assertFalse(os.path.exists(run_config['summary_file_path']))
        self.assertTrue(hasattr(cmu, 'bin_stats_data'))

    def test_03_filter_binned_contigs_all_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_all_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        # set filters so all will pass => returns none
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Theta',
            'completeness_perc': 95.0,
            'contamination_perc': 2.0,
            'workspace_name': self.wsName,
        }))
        # no summary file
        self.assertFalse(os.path.exists(run_config['summary_file_path']))
        self.assertTrue(hasattr(cmu, 'bin_stats_data'))

    def check_filtered_bins(self, cmu, run_config, results, expected):

        for key, value in expected.items():
            self.assertEqual(results[key], value)

        self.assertTrue('filtered_obj_ref' in results)

        # summary file has been created
        self.assertTrue(os.path.exists(run_config['summary_file_path']))
        self.assertTrue(hasattr(cmu, 'bin_stats_data'))
        expected_content_file = os.path.join(
            'data', 'expected', expected['filtered_obj_name'] + '.summary'
        )
        # check that the summary file is as expected
        self.assertEqual(
                open(run_config['summary_file_path'], 'r').read(),
                open(expected_content_file, 'r').read()
            )

        # filtered bins dir has been created
        self.assertTrue(os.path.exists(run_config['filtered_bins_dir']))
        for id in expected['retained_bin_IDs'].keys():
            expected_path = os.path.join(
                run_config['filtered_bins_dir'], id + '.' + run_config['fasta_ext']
            )
            self.assertTrue(os.path.isfile(expected_path))
        #

    def test_03_filter_binned_contigs_some_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_some_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        # 002 will pass
        results = cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Epsilon',
            'completeness_perc': 95.0,  # all pass
            'contamination_perc': 1.5,  # 002 passes
            'workspace_name': self.wsName,
        })
        expected = {
            'filtered_obj_name': 'Epsilon',
            'retained_bin_IDs': {'bin.002': True},
            'removed_bin_IDs':  {'bin.001': True, 'bin.003': True}
        }
        self.check_filtered_bins(cmu, run_config, results, expected)

    def test_03_filter_binned_contigs_some_others_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_some_others_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        # 001 and 002 will pass
        results = cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Gamma',
            'completeness_perc': 97.0,  # 001 and 002 pass
            'contamination_perc': 2.5,  # all pass
            'workspace_name': self.wsName,
        })
        expected = {
            'filtered_obj_name': 'Gamma',
            'retained_bin_IDs': {'bin.001': True, 'bin.002': True},
            'removed_bin_IDs':  {'bin.003': True},
        }
        self.check_filtered_bins(cmu, run_config, results, expected)
