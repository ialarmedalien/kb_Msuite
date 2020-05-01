import os
import shutil
import re
import ast
import sys
import time
import json
import csv

from kb_Msuite.Utils.Utils import Base, LogMixin, TSVMixin

class OutputBuilder(Base, LogMixin, TSVMixin):
    '''
    Constructs the output HTML report and artifacts based on a CheckM lineage_wf
    run.  This includes running any necssary plotting utilities of CheckM.
    '''

    # (self, run_config)
    def __init__(self, checkMUtil_obj):
        self.checkMUtil     = checkMUtil_obj
        self.client_util    = checkMUtil_obj.client_util
        self.scratch        = checkMUtil_obj.scratch
        self.PLOT_FILE_EXT  = '.ref_dist_plots.png'

    def client(self, client_name):

        return self.client_util.client(client_name)

    def run_config(self):

        return self.checkMUtil.run_config()

    def build_report(self, params, filtered_obj_info=None):

        run_config = self.run_config()

        binned_contig_obj_ref = None
        removed_bins = None

        if filtered_obj_info:
            binned_contig_obj_ref   = filtered_obj_info['filtered_obj_ref']
            removed_bins            = filtered_obj_info['removed_bin_IDs']

        report_params = {
            'report_object_name': 'kb_checkM_report_' + run_config['suffix'],
            'workspace_name': params['workspace_name'],
        }

        self.logger.info('Packaging output directory')
        output_packages = [{
            'name': 'full_output',
            'path': run_config['output_dir'],
            'description': 'Full output of CheckM',
        }]

        if hasattr(self.checkMUtil, 'bin_stats_data'):
            bin_stats_data = self.checkMUtil.bin_stats_data
        else:
            bin_stats_data = self.read_bin_stats_file()

        if bin_stats_data:
            self.logger.info('creating HTML and TSV summary tables')
            html_links = self.build_html_output_for_lineage_wf(bin_stats_data, params, removed_bins)
            report_params['direct_html_link_index'] = 0
            report_params['html_links'] = html_links

#         else:  # ADD LATER?
#             log('not packaging full output directory, selecting specific files')
#             crit_out_dir = os.path.join(self.scratch,
#                 'critical_output_' + os.path.basename(input_dir))
#             os.makedirs(crit_out_dir)

            output_packages.append({
                'name': run_config['tab_text_file_name'],
                'path': run_config['tab_text_file'],
                'description': 'TSV Summary Table from CheckM',
            })

            if 'save_plots_dir' in params:
                self.logger.info('packaging output plots directory')
                output_packages.append({
                    'name': 'plots',
                    'path': run_config['plots_dir'],
                    'description': 'Output plots from CheckM',
                })
            else:
                self.logger.info('not packaging output plots directory')

            if binned_contig_obj_ref:
                report_params['objects_created'] = [{
                    'ref':          binned_contig_obj_ref,
                    'description':  'HQ BinnedContigs ' + filtered_obj_info['filtered_obj_name']
                }]

        else:
            self.logger.warning('CheckM produced no output!')
            report_params['message'] = 'CheckM did not produce any output.'

        report_params['file_links'] = output_packages

        kr = self.client('KBaseReport')
        report_output = kr.create_extended_report(report_params)

        returnVal = {
            'report_name': report_output['name'],
            'report_ref':  report_output['ref'],
        }

        if binned_contig_obj_ref:
            returnVal.update({'binned_contig_obj_ref': binned_contig_obj_ref})

        return returnVal

    # requires the stats file - self.output_dir, 'storage', 'bin_stats_ext.tsv'
    def build_html_output_for_lineage_wf(self, bin_stats, params, removed_bins=None):

        '''
        Based on the output of CheckM lineage_wf, build an HTML report
        '''
        run_config      = self.run_config()
        html_dir        = run_config['html_dir']
        plots_dir       = run_config['plots_dir']
        html_plots_dir  = os.path.join(html_dir, 'plots')
        tab_text_dir    = run_config['tab_text_dir']
        tmpl_src_dir    = run_config['template_src_dir']
        tmpl_dest_dir   = run_config['template_dest_dir']

        for dir in [html_plots_dir, tab_text_dir, tmpl_dest_dir]:
            os.makedirs(dir, exist_ok=True)

        # copy over the templates
        for tmpl in ['dist_html_page.tt', 'checkM_table.tt']:
            tmpl_file = os.path.join(tmpl_dest_dir, tmpl)
            if not os.path.exists(tmpl_file) or not os.path.isfile(tmpl_file):
                self._copy_file_ignore_errors(tmpl, tmpl_src_dir, tmpl_dest_dir)

        html_index_file = os.path.join(html_dir, 'checkm_results.html')

        html_files = [
            {
                # checkm table:
#                 'template': {
#                     'template_file': os.path.join(tmpl_dest_dir, 'checkM_table.tt'),
#                     'template_data_json': {'params':params,}
#                 },
                'name': 'checkm_results.html',
                'description': 'Summarized report from CheckM',
                'path': html_index_file,
            },{
                'name': run_config['tab_text_file_name'],
                'path': run_config['tab_text_file'],
            },{
                'name': 'plots',
                'path': html_plots_dir,
            }
        ]

        results_filtered = 'results_filtered' in run_config

        # init the TSV output file
        with open(run_config['tab_text_file'], 'w', newline='') as tab_text_fh:
            tsv_writer = self.init_tsv_writer(tab_text_fh)
            self.write_tsv_headers(tsv_writer, results_filtered)

            # init html_file output
            with open(html_index_file, 'w') as html_index_fh:

                for bid in sorted(bin_stats.keys()):
                    bin_id = self.checkMUtil.clean_bin_ID(bid)
                    bin_stats[bid]['Bin Name'] = bin_id

                    # bin_id = re.sub('^[^\.]+\.', '', bid)
                    if removed_bins:
                        bin_stats[bid]['QA Pass'] = False if bin_id in removed_bins else True

                    self.logger.debug({
                        'bid': bid,
                        'bin_id': bin_id,
                        'bin_stats': bin_stats[bid]
                    })

                    # create the dist plot page
                    plot_file = os.path.join(plots_dir, str(bid) + self.PLOT_FILE_EXT)
                    bin_stats[bid]['Has Plot File'] = False
                    if os.path.isfile(plot_file):
                        bin_stats[bid]['Has Plot File'] = True
                        html_dir_plot_file = os.path.join(
                            html_plots_dir, str(bid) + self.PLOT_FILE_EXT
                        )
                        # copy it to the html_plot
                        self._copy_file_new_name_ignore_errors(plot_file, html_dir_plot_file)
                        html_files.append({
                            'template': {
                                'template_data_json': json.dumps({
                                    'bin_id': bin_id,
                                    'plot_file_ext': self.PLOT_FILE_EXT,
                                }),
                                'template_file': os.path.join(tmpl_dest_dir, 'dist_html_page.tt'),
                            },
                            'name': bin_id + '.html',
                        })

                    self.write_tsv_row(tsv_writer, bid, bin_stats[bid], results_filtered)
                    html_index_fh.write({'bin_id': bin_id, 'bin_stats': bin_stats[bid]})

        return html_files

    def get_fields(self, results_filtered):
        tsv_fields = [
            {'id': 'Bin Name'},
            {'id': 'marker lineage', 'display': 'Marker Lineage'},
            {'id': '# genomes', 'display': '# Genomes'},
            {'id': '# markers', 'display': '# Markers'},
            {'id': '# marker sets', 'display': '# Marker Sets'},
            {'id': '0'},
            {'id': '1'},
            {'id': '2'},
            {'id': '3'},
            {'id': '4'},
            {'id': '5+'},
            {'id': 'Completeness', 'round': 2},
            {'id': 'Contamination', 'round': 2},
            {'id': 'Has Plot File'},
        ]

        if results_filtered:
            return tsv_fields + [{'id': 'QA Pass'}]

        return tsv_fields

    def write_tsv_headers(self, tsv_writer, results_filtered):

        tsv_fields = self.get_fields(results_filtered)
        headers = [f.get('display', f['id']) for f in tsv_fields]
        tsv_writer.writerow(headers)

    def write_tsv_row(self, tsv_writer, bid, bin_stats, results_filtered):

        row = [bid]
        fields = self.get_fields(results_filtered)
        for f in fields:
            if f['id'] in bin_stats:
                if f.get('round'):
                    row.append(str(round(bin_stats[f['id']], f['round'])))
                else:
                    row.append(str(bin_stats[f['id']]))
            else:
                row.append('')

        tsv_writer.writerow(row)

    def read_bin_stats_file(self):
        run_config = self.run_config()
        stats_file = run_config['bin_stats_ext_file']

        bin_stats = dict()

        if not os.path.isfile(stats_file):
            self.logger.warning('No stats file found (looking at: ' + stats_file + ')')
            return bin_stats

        with open(stats_file) as lf:
            for line in lf:
                if not line:
                    continue
                if line.startswith('#'):
                    continue
                col = line.split('\t')
                bin_id = str(col[0])
                data = ast.literal_eval(col[1])
                bin_stats[bin_id] = data

        return bin_stats

    def _copy_file_ignore_errors(self, filename, src_folder, dest_folder):

        src  = os.path.join(src_folder, filename)
        dest = os.path.join(dest_folder, filename)
        os.makedirs(dest_folder, exist_ok=True)

        return self._copy_file_new_name_ignore_errors(src, dest)

    def _copy_file_new_name_ignore_errors(self, source_path, destination_path):
        # self.logger.debug('copying ' + source_path + ' to ' + destination_path)
        try:
            shutil.copy(source_path, destination_path)
        except Exception as e:
            # TODO: add error message reporting
            self.logger.error('copy failed')
            self.logger.error(e)
