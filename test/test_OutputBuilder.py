import os
import shutil
import csv
from pathlib import Path

from kb_Msuite.Utils.Utils import TSVMixin
from kb_Msuite.Utils.FileUtils import read_bin_stats_file

from CheckMTestBase import CoreCheckMTestClient, CheckReportMixin


class TestOutputBuilder(CoreCheckMTestClient, CheckReportMixin, TSVMixin):

    def mimic_tsv_output(self, cmu, tab_text_file):

        run_config = cmu.run_config()
        results_filtered = run_config['results_filtered']
        with open(tab_text_file, 'w', newline='') as tab_text_fh:
            tsv_writer = cmu.outputbuilder.init_tsv_writer(tab_text_fh)
            cmu.outputbuilder.write_tsv_headers(tsv_writer, results_filtered)

    def notest_05_write_tsv_headers(self):

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        os.makedirs(run_config['base_dir'], exist_ok=True)

        with self.subTest('No output filters in place'):
            filename = os.path.join(run_config['base_dir'], 'tab_text_no_filter.tsv')
            self.mimic_tsv_output(cmu, filename)
            # now read in the file and check the data is correct
            with open(filename, 'r') as tab_text_fh:
                lines = tab_text_fh.read().rstrip()
            self.assertRegex(lines, r'Has Plot File$')

        with self.subTest('Output filtered'):
            run_config['results_filtered'] = True
            filename = os.path.join(run_config['base_dir'], 'tab_text_with_filter.tsv')
            self.mimic_tsv_output(cmu, filename)
            with open(filename, 'r', newline='') as tab_text_fh:
                lines = tab_text_fh.read().rstrip()
            # the last col will be 'QA Pass'
            self.assertRegex(lines, r'QA Pass$')

    def notest_05_outputbuilder_no_checkM_output(self):

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        # no checkM output: no report
        os.makedirs(run_config['output_dir'])
        Path(os.path.join(run_config['output_dir'], 'checkm.log')).touch(exist_ok=True)
        params = {
            'workspace_name': self.ws_info[1],
            'save_plots_dir': 1,
        }
        report = cmu.outputbuilder.build_report(params)

        expected_results = {
            'file_links': ['full_output'],
            'text_message': 'CheckM did not produce any output.',
        }
        self.check_report(report, expected_results)

    def notest_05_outputbuilder_genome_assembly_set(self):

        self.require_data('assembly_set_small_ref')

        cmu = self.checkMUtil
        run_config = cmu.run_config()

        # prep the data
        os.makedirs(run_config['base_dir'], exist_ok=True)
        for dir in ['output', 'plots']:
            shutil.copytree(
                os.path.join('data', 'results', 'assemblyset', dir),
                os.path.join(run_config['base_dir'], dir)
            )

        assemblies = self.get_data()['assembly_list'][0:3]

        bin_stats_data = read_bin_stats_file(run_config['bin_stats_ext_file'])
        params = {}
        html_files = cmu.outputbuilder.build_html_tsv_files(bin_stats_data, params)

        # html_files[0] is the summary template
        self.assertEqual(html_files[0]['name'], 'checkm_results.html')
        self.assertTrue('template' in html_files[0])

        # html_files[1] is the TSV file
        self.assertEqual(html_files[1], {
            'name': run_config['tab_text_file_name'],
            'path': run_config['tab_text_file'],
        })
        expected_content_file = os.path.join(
            'data', 'results', 'assemblyset', 'summary_table.tsv'
        )
        result_lines = open(html_files[1]['path'], 'r').read().splitlines(keepends=False)
        expected_lines = open(expected_content_file, 'r').read().splitlines(keepends=False)
        self.assertEqual(result_lines, expected_lines)

        # html_files[2] is the html plots dir
        self.assertEqual(html_files[2], {
            'name': 'plots',
            'path': os.path.join(run_config['html_dir'], 'plots')
        })
        # ensure that the plots dir contains the correct plots
        plot_file_ext = cmu.outputbuilder.PLOT_FILE_EXT
        for a in assemblies:
            plot_file = os.path.join(run_config['html_dir'], 'plots', a['name'] + plot_file_ext)
            self.assertTrue(os.path.isfile(plot_file))

        # 3-5 are the dist files
        self.assertEqual(
            set(a['name'] + '.html' for a in assemblies),
            set(h['name'] for h in html_files[3:]),
        )

    def notest_05_outputbuilder_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 05_outputbuilder_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref')

        cmu = self.checkMUtil
        run_config = cmu.run_config()

        # 'bin.010' has no plot file
        ids_with_plots = ['002', '005', '006', '009', '014', '033']

        dist_files = [run_config['bin_basename'] + '.' + id + '.html' for id in ids_with_plots]
        # lots of output:
        with self.subTest('binned contig, no filtering'):
            os.makedirs(run_config['base_dir'], exist_ok=True)
            for dir in ['output', 'plots']:
                shutil.copytree(
                    os.path.join('data', 'results', 'binned_contigs', dir),
                    os.path.join(run_config['base_dir'], dir)
                )

            params = {
                'workspace_name': self.ws_info[1],
                'save_plots_dir': 1,
            }
            result = cmu.outputbuilder.build_report(params)
            self.assertEqual(set(result.keys()), set(['report_name', 'report_ref']))

            expected_results = {
                'direct_html_link_index': 0,
                'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
                'html_links': [
                    'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                ] + dist_files,
            }
            self.check_report(result, expected_results)
            # check the TSV file: there should be no QA Pass col
            with open(run_config['tab_text_file'], newline='') as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                for row in reader:
                    self.logger.debug({'row': row})
                    # if the bin has a plot file, 'Has Plot File' will be true.
                    has_plot_file = 'True' if row['Bin Name'] + '.html' in dist_files else 'False'
                    self.assertEqual(row['Has Plot File'], has_plot_file)
                    self.assertNotIn('QA Pass', row)

        with self.subTest('binned contig, filters, new object created'):

            run_config['results_filtered'] = True
            for dir in ['html_dir', 'tab_text_dir', 'template_dest_dir']:
                shutil.rmtree(run_config[dir], ignore_errors=True)

            params = {'workspace_name': self.ws_info[1]}
            removed_bin_IDs = ['bin.002', 'bin.005', 'bin.033']
            filtered_obj_info = {
                'filtered_obj_ref': self.binned_contigs_ref,
                'filtered_obj_name': 'Nancy Drew',
                'removed_bin_IDs': {
                    bin_ID: True for bin_ID in removed_bin_IDs
                },
            }
            self.logger.debug({'filtered_obj_info': filtered_obj_info})

            result = cmu.outputbuilder.build_report(params, filtered_obj_info)

            self.assertEqual(
                set(result.keys()),
                set(['report_name', 'report_ref', 'binned_contig_obj_ref'])
            )
            self.assertEqual(result['binned_contig_obj_ref'], self.binned_contigs_ref)
            expected_results = {
                'direct_html_link_index': 0,
                # note: no plots dir
                'file_links': ['CheckM_summary_table.tsv', 'full_output'],
                'html_links': [
                    'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                ] + dist_files,
                'objects_created': [{
                    'ref':         filtered_obj_info['filtered_obj_ref'],
                    'description': 'HQ BinnedContigs ' + filtered_obj_info['filtered_obj_name'],
                }]
            }

            self.check_report(result, expected_results)
            # there should be one bin with a missing plot file
            with open(run_config['tab_text_file'], newline='') as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                for row in reader:
                    self.logger.debug({'row': row})
                    # if the bin has a plot file, 'Has Plot File' will be true.
                    has_plot_file = 'True' if row['Bin Name'] + '.html' in dist_files else 'False'
                    self.assertEqual(row['Has Plot File'], has_plot_file)
                    # if the bin is in 'removed_bin_IDs', it will have failed QA
                    qa_pass = 'False' if row['Bin Name'] in removed_bin_IDs else 'True'
                    self.assertEqual(row['QA Pass'], qa_pass)
