# -*- coding: utf-8 -*-
import os
import shutil
import logging
from core_CheckM_test import CoreCheckMTestClient


class CheckMEndToEnd(CoreCheckMTestClient):

    def run_and_check_report(self, params, expected=None, with_filters=False):
        '''
        Run 'run_checkM_lineage_wf' with or without filters, and check the resultant KBaseReport
        using check_report()

        Args:

          params        - dictionary of input params
          expected      - dictionary representing the expected structure of the KBaseReport object
          with_filters  - whether or not to use the 'withFilter' version of the workflow

        '''
        if (with_filters):
            result = self.getImpl().run_checkM_lineage_wf_withFilter(self.getContext(), params)[0]
        else:
            result = self.getImpl().run_checkM_lineage_wf(self.getContext(), params)[0]

        return self.check_report(result, expected)

    def check_report(self, result, expected):
        '''
        Test utility to check a KBaseReport object
        Args:

          result    - result returned by running KBaseReport.get_extended_report
                      { 'report_name': blahblahblah, 'report_ref': reference }

          expected  - dictionary representing the expected structure of the report
                      any keys omitted from the dictionary are assumed to be the report default
                      (None or an empty list)
        '''

        self.assertIn('report_name', result)
        self.assertIn('report_ref', result)

        # make sure the report was created and includes the HTML report and download links
        got_object = self.getWsClient().get_objects2({
            'objects': [{'ref': result['report_ref']}]
        })
        rep = got_object['data'][0]['data']
        self.logger.info({'report data': rep})

        report_data = {
            'text_message': None,
            'file_links': [],
            'html_links': [],
            'warnings': [],
            'direct_html': None,
            'direct_html_link_index': None,
            'objects_created': [],
            'html_window_height': None,
            'summary_window_height': None,
        }
        report_data.update(expected)

        for key in expected.keys():
            with self.subTest('checking ' + key):
                if key == 'file_links' or key == 'html_links':
                    self.check_report_links(rep, key, report_data)
                elif key == 'objects_created' and expected['objects_created']:
                    # 'objects_created': [{
                    #   'description': 'HQ BinnedContigs filter.BinnedContigs',
                    #   'ref': '50054/17/1'
                    # }]
                    self.assertTrue(len(rep['objects_created']) == 1)
                    obj = rep['objects_created'][0]
                    self.assertTrue(len(obj.keys()) == 2)
                    self.assertEqual(obj['description'], 'HQ BinnedContigs filter.BinnedContigs')
                    self.assertRegex(obj['ref'], r'\d+/\d+/\d+')
                else:
                    self.assertEqual(rep[key], report_data[key])

        return True

    def check_report_links(self, report_obj, type, expected):
        """
        Test utility: check the file upload results for an extended report
        Args:
          report_obj    - result dictionary from running KBaseReport.create_extended_report
          type          - one of "html_links" or "file_links"
          file_names    - names of the files for us to check against
        """
        file_links = report_obj[type]
        self.assertEqual(len(file_links), len(expected[type]))
        # Test that all the filenames listed in the report object map correctly
        saved_names = set([str(f['name']) for f in file_links])
        self.assertEqual(saved_names, set(expected[type]))
        return True

    def test_checkM_end_to_end_errors(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_errors")
        self.logger.info("=================================================================\n")

        with self.subTest('invalid workspace ref'):
            # invalid ref
            params = {
                'workspace_name': self.ws_info[1],
                'reduced_tree': 1,
                'input_ref': 'here_is_a_made_up_ref',
            }
            err_msg = 'Cannot retrieve object "here_is_a_made_up_ref". Dying.'
            with self.assertRaisesRegex(ValueError, err_msg):
                self.getImpl().run_checkM_lineage_wf(self.getContext(), params)

        with self.subTest('incorrect object type'):
            # incorrect object type
            params = {
                'workspace_name': self.ws_info[1],
                'reduced_tree': 1,
                'input_ref': self.report_ref,
            }
            err_str = 'Cannot stage fasta file input directory from type: KBaseReport.Report'
            with self.assertRaisesRegex(ValueError, err_str):
                self.getImpl().run_checkM_lineage_wf(self.getContext(), params)

    # Test 1: single assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_single_assembly")
    def notest_checkM_end_to_end_single_assembly(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_single_assembly")
        self.logger.info("=================================================================\n")

        # run checkM lineage_wf app on a single assembly
        assembly = self.get_data()['assembly_list'][2]  # assembly B, 654KB
        self.require_data(assembly['attr'])

        input_ref = getattr(self, assembly['attr'])
        params = {
            'dir_name': assembly['attr'],
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 0,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                assembly['name'] + '.html'
            ],
        }

        self.run_and_check_report(params, expected_results)

    # Test 2: Regression test (CheckM <= v1.0.7) for single problem assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_single_problem_assembly")
    def notest_checkM_end_to_end_single_problem_assembly(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_single_problem_assembly")
        self.logger.info("=================================================================\n")

        # regression test for contig that breaks checkm v1.0.7 reduced_tree
        # (works on v1.0.8)
        # 'path': 'offending_contig_67815-67907.fa',
        # 'name': 'Dodgy_Contig.Assembly',
        # 'attr': 'assembly_dodgy_ref',
        assembly = self.get_data()['assembly_list'][4]
        self.require_data(assembly['attr'])

        input_ref = getattr(self, assembly['attr'])
        params = {
            'dir_name': assembly['attr'],
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,  # this must be 1 to regression test with --reduced_tree
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                assembly['name'] + '.html'
            ],
        }

        self.run_and_check_report(params, expected_results)

    # Test 3: binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_binned_contigs")
    def notest_checkM_end_to_end_binned_contigs(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_binned_contigs")
        self.logger.info("=================================================================\n")

        # run checkM lineage_wf app on BinnedContigs
        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory
        binned_contigs = self.get_data()['binned_contigs_list'][0]
        self.require_data(binned_contigs['attr'])

        input_ref = getattr(self, binned_contigs['attr'])
        params = {
            'dir_name': binned_contigs['attr'],
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        bin_html_files = ['bin.' + n + '.html' for n in ['001', '002', '003']]
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
            ] + bin_html_files,
        }

        self.run_and_check_report(params, expected_results)

    # Test 4: Regression test for empty binned contigs object
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_binned_contigs_EMPTY")
    def notest_checkM_end_to_end_binned_contigs_EMPTY(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_binned_contigs_EMPTY")
        self.logger.info("=================================================================\n")

        # run checkM lineage_wf app on EMPTY BinnedContigs
        binned_contigs = self.get_data()['binned_contigs_list'][1]
        self.require_data(binned_contigs['attr'])

        input_ref = getattr(self, binned_contigs['attr'])
        params = {
            'dir_name': binned_contigs['attr'],
            'workspace_name': self.ws_info[1],
            'reduced_tree': 1,
            'input_ref': input_ref
        }
        with self.assertRaises(ValueError) as exception_context:
            self.getImpl().run_checkM_lineage_wf(self.getContext(), params)
        self.assertTrue('Binned Assembly is empty' in str(exception_context.exception))

    # Test 5: Assembly Set
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_assemblySet")
    def notest_checkM_end_to_end_assemblySet(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_assemblySet")
        self.logger.info("=================================================================\n")

        # run checkM lineage_wf app on an assembly set
        self.require_data('assembly_set_small_ref')
        assemblyset = self.get_data()['assemblyset_list'][0]

        input_ref = getattr(self, assemblyset['attr'])
        params = {
            'dir_name': assemblyset['attr'],
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        # assemblyset['items'] looks like this:
        # { 'ref': assembly['attr'], 'label': assembly['name'] }
        html_files = [a['name'] + '.html' for a in assemblyset['items']]
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
            ] + html_files,
        }

        self.run_and_check_report(params, expected_results)

    # Test 6: Single Genome
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_single_genome")
    def notest_checkM_end_to_end_single_genome(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_single_genome")
        self.logger.info("=================================================================\n")

        # run checkM lineage_wf app on a single genome
        genome = self.get_data()['genome'][0]
        self.require_data(genome['attr'])

        input_ref = getattr(self, genome['attr'])
        params = {
            'dir_name': genome['attr'],
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                genome['name'] + '.html'
            ],
        }
        # correct the file name!
        self.run_and_check_report(params, expected_results)

    # Test 7: Genome Set
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_genomeSet")
    def notest_checkM_end_to_end_genomeSet(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_genomeSet")
        self.logger.info("=================================================================\n")

        # run checkM lineage_wf app on a genome set
        self.require_data('genome_set_small_ref')
        genomeset = self.get_data()['genomeset_list'][0]

        input_ref = getattr(self, genomeset['attr'])
        params = {
            'dir_name': genomeset['attr'],
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        # genomeset['items'] is a dict in the form
        # genome['name']: { 'ref': genome['attr'] }
        html_files = [a + '.html' for a in genomeset['items'].keys()]
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
            ] + html_files,
        }

        self.run_and_check_report(params, expected_results)

    # Test 11: filter binned contigs to HQ binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_filter_binned_contigs")
    def notest_checkM_lineage_wf_withFilter_binned_contigs(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_withFilter_binned_contigs")
        self.logger.info("=================================================================\n")

        # run checkM lineage_wf app on BinnedContigs, with filters!
        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory
        binned_contigs = self.get_data()['binned_contigs_list'][0]
        self.require_data(binned_contigs['attr'])

        input_ref = getattr(self, binned_contigs['attr'])
        params = {
            'dir_name': 'binned_contigs_filter',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'completeness_perc': 95.0,
            'contamination_perc': 1.5,
            'output_filtered_binnedcontigs_obj_name': 'BinnedContigs_filtered',
            'threads': 4
        }

        bin_html_files = ['bin.' + n + '.html' for n in ['001', '002', '003']]
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
            ] + bin_html_files,
            # newly created binned contigs object!
            'objects_created': True,

        }

        self.run_and_check_report(params, expected_results, True)

    def setup_local_method_data(self):

        base_dir = os.path.dirname(__file__)
        test_data_dir = os.path.join(base_dir, 'data', 'example-bins')
        scratch_input_dir = os.path.join(self.scratch, 'lineage_wf_input_dir')
        scratch_output_dir = os.path.join(
            self.scratch, 'lineage_wf_output_dir' + '_' + str(self.suffix)
        )
        shutil.copytree(test_data_dir, scratch_input_dir)
        if not os.path.exists(scratch_output_dir):
            os.mkdir(scratch_output_dir)
        log_path = os.path.join(self.scratch, 'lineage_wf.log')
        return scratch_input_dir, scratch_output_dir, log_path

    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_local_method()")
    def notest_04_local_method(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING 04_local_method")
        self.logger.info("=================================================================\n")

        """
        Test a successful run of the .lineage_wf local method
        This just does some very basic testing to make sure the executable runs.
        """
        input_dir, output_dir, log_path = self.setup_local_method_data()
        self.getImpl().lineage_wf(self.getContext(), {
            'input_dir': input_dir,
            'output_dir': output_dir,
            'log_path': log_path,
            'options': {
                '-x': 'fasta',
                '--reduced_tree': ''
            }
        })
        out_contents = sorted(os.listdir(output_dir))
        # self.assertEqual(out_contents, ['storage', 'lineage.ms', 'bins'])
        self.assertEqual(out_contents, ['bins', 'checkm.log', 'lineage.ms', 'storage'])
        self.assertTrue(os.path.exists(log_path))
        # Remove test data
        os.remove(log_path)
        shutil.rmtree(input_dir)
        shutil.rmtree(output_dir)


def print_method_name(method):
    def wrapper(*args, **kwargs):
        method_name = method.__name__
        method_name.replace("test_", "")
        logger = logging.getLogger('kb_Msuite.CoreCheckMTest')
        logger.info("=================================================================")
        logger.info("RUNNING " + method_name)
        logger.info("=================================================================\n")
        return method(*args, **kwargs)
    return wrapper
