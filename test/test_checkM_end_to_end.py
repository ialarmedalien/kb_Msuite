import unittest
import os
import shutil
from CheckMTestBase import CoreCheckMTestClient, CheckReportMixin


class CheckMEndToEnd(CoreCheckMTestClient, CheckReportMixin):

    def test_checkM_end_to_end_errors(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_errors")
        self.logger.info("=================================================================\n")

        self.require_data('report_ref')

        with self.subTest('invalid workspace ref'):
            # invalid ref
            params = {
                'workspace_name': self.ws_info[1],
                'reduced_tree': 1,
                'input_ref': 'here_is_a_made_up_ref',
            }
            err_msg = 'Unable to perform workspace command "get_object_info3": '
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
    @unittest.skip("skipped test_checkM_end_to_end_single_assembly")
    def test_checkM_end_to_end_single_assembly(self):

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
    @unittest.skip("skipped test_checkM_end_to_end_single_problem_assembly")
    def test_checkM_end_to_end_single_problem_assembly(self):
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
    @unittest.skip("skipped test_checkM_end_to_end_binned_contigs")
    def test_checkM_end_to_end_binned_contigs(self):
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
    @unittest.skip("skipped test_checkM_end_to_end_binned_contigs_EMPTY")
    def test_checkM_end_to_end_binned_contigs_EMPTY(self):
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
    @unittest.skip("skipped test_checkM_end_to_end_assemblySet")
    def test_checkM_end_to_end_assemblySet(self):
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
    @unittest.skip("skipped test_checkM_end_to_end_single_genome")
    def test_checkM_end_to_end_single_genome(self):
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
    @unittest.skip("skipped test_checkM_end_to_end_genomeSet")
    def test_checkM_end_to_end_genomeSet(self):
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
    @unittest.skip("skipped test_checkM_end_to_end_filter_binned_contigs")
    def test_checkM_lineage_wf_withFilter_binned_contigs(self):
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
    @unittest.skip("skipped test_local_method()")
    def test_04_local_method(self):
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
