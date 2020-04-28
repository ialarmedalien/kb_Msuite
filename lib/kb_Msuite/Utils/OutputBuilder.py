import os
import shutil
import re
import ast
import sys
import time
import json
import csv

from kb_Msuite.Utils.Logger import Base, LogMixin

def log(message, prefix_newline=False):
    """Logging function, provides a hook to suppress or redirect log messages."""
    print(('\n' if prefix_newline else '') + '{0:.2f}'.format(time.time()) + ': ' + str(message))
    sys.stdout.flush()


class OutputBuilder(Base, LogMixin):
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

    def get_fields(self):
        return [
            {'id': 'marker lineage', 'display': 'Marker Lineage'},
            {'id': '# genomes', 'display': '# Genomes'},
            {'id': '# markers', 'display': '# Markers'},
            {'id': '# marker sets', 'display': '# Marker Sets'},
            {'id': '0', 'display': '0'},
            {'id': '1', 'display': '1'},
            {'id': '2', 'display': '2'},
            {'id': '3', 'display': '3'},
            {'id': '4', 'display': '4'},
            {'id': '5+', 'display': '5+'},
            {'id': 'Completeness', 'display': 'Completeness', 'round': 2},
            {'id': 'Contamination', 'display': 'Contamination', 'round': 2}
        ]

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


        log('Packaging output directory')
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
            # create bin report summary TSV table text file
            log('creating TSV summary table text file')
            # self.build_summary_tsv_file(bin_stats_data, removed_bins)

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

            if 'save_plots_dir' in params and str(params['save_plots_dir']) == '1':
                log('packaging output plots directory')
                output_packages.append({
                    'name': 'plots',
                    'path': run_config['plots_dir'],
                    'description': 'Output plots from CheckM',
                })
            else:
                log('not packaging output plots directory')

            if binned_contig_obj_ref:
                report_params['objects_created'] = [{
                    'ref':          binned_contig_obj_ref,
                    'description':  'HQ BinnedContigs ' + filtered_obj_info['filtered_obj_name']
                }]

        else:
            log("WARNING: No output produced!")
            self.logger.warning('WARNING: checkM produced no output!')
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
        with open(run_config['tab_text_file'], 'w') as tsv_file:
            header = self._generate_row_header(results_filtered)
            tsv_file.write("\t".join(header)+"\n")

            # init html_file output
            with open(html_index_file, 'w') as open_fh:

                for bid in sorted(bin_stats.keys()):
                    # DEBUG

                    bin_id = re.sub('^[^\.]+\.', '', bid)
                    if removed_bins and bin_id in removed_bins:
                        self.logger.debug("BIN STATS BID " + bid + ": REMOVED")

                        log("BIN STATS BID " + bid + ": REMOVED")
                    else:
                        self.logger.debug("BIN STATS BID " + bid)
                        log("BIN STATS BID " + bid)

                    # create the dist plot page
                    plot_file = os.path.join(plots_dir, str(bid) + self.PLOT_FILE_EXT)
                    has_plot_file = False
                    if os.path.isfile(plot_file):
                        has_plot_file = True
                        html_dir_plot_file = os.path.join(html_plots_dir, str(bid) + self.PLOT_FILE_EXT)
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

                    row = self._generate_row_data(
                        bid, bin_stats[bid], has_plot_file, results_filtered, removed_bins
                    )
                    tsv_file.write("\t".join(row) + "\n")
                    open_fh.write("BIN ID: " + bin_id + "\t".join(row) + "\n")

        return html_files

    def _generate_row_header(self, results_filtered):
        out_header = ['Bin Name']
        fields = self.get_fields()
        for f in fields:
            out_header.append(f['display'])
        out_header.append('has_plot_file')
        if results_filtered:
            out_header.append('QC Pass')
        return out_header

    def _generate_row_data(self, bid, bin_stats, has_plot_file, results_filtered, removed_bins):

        row = [bid]
        fields = self.get_fields()
        for f in fields:
            if f['id'] in bin_stats:
                value = str(bin_stats[f['id']])
                if f.get('round'):
                    value = str(round(bin_stats[f['id']], f['round']))
                row.append(str(value))

        # is there a plot file for this entry?
        if has_plot_file:
            row.append('true')
        else:
            row.append('false')

        # add a column to indicate whether the bin should be removed
        if results_filtered:
            if removed_bins:
                bin_id = re.sub('^[^\.]+\.', '', bid)
                if bin_id in removed_bins:
                    row.append('false')
                else:
                    row.append('true')
            else:
                row.append('true')

        return row

    def read_bin_stats_file(self):
        run_config = self.run_config()
        stats_file = run_config['bin_stats_ext_file']

        bin_stats = dict()

        if not os.path.isfile(stats_file):
            log('Warning! no stats file found (looking at: ' + stats_file + ')')
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
        log('copying ' + source_path + ' to ' + destination_path)
        try:
            shutil.copy(source_path, destination_path)
        except Exception as e:
            # TODO: add error message reporting
            log('copy failed')
            log(e)

#     def build_critical_output(self, critical_out_dir):
#
#         run_config = self.run_config()
#         self._copy_file_ignore_errors('lineage.ms', run_config['output_dir'], critical_out_dir)
#
#         storage_folder = os.path.join(critical_out_dir, 'storage')
#         if not os.path.exists(storage_folder):
#             os.makedirs(storage_folder)
#
#         src = run_config['output_dir']
#         dest = critical_out_dir
#         self._copy_file_ignore_errors(os.path.join('storage', 'bin_stats.analyze.tsv'), src, dest)
#         self._copy_file_ignore_errors(os.path.join('storage', 'bin_stats.tree.tsv'), src, dest)
#         self._copy_file_ignore_errors(os.path.join('storage', 'bin_stats_ext.tsv'), src, dest)
#         self._copy_file_ignore_errors(os.path.join('storage', 'marker_gene_stats.tsv'), src, dest)
#         self._copy_file_ignore_errors(os.path.join('storage', 'tree', 'concatenated.tre'), src, dest)

#     def _copy_ref_dist_plots(self, dest_folder):
#         run_config = self.run_config()
#         plots_dir = run_config['plots_dir']
#         for plotfile in os.listdir(plots_dir):
#             plot_file_path = os.path.join(plots_dir, plotfile)
#             if os.path.isfile(plot_file_path) and plotfile.endswith(self.PLOT_FILE_EXT):
#                 try:
#                     shutil.copy(os.path.join(plots_dir, plotfile),
#                                 os.path.join(dest_folder, plotfile))
#                 except:
#                     # TODO: add error message reporting
#                     log('copy of ' + plot_file_path + ' to html directory failed')
#     def _write_dist_html_page(self, html_dir, bin_id):
#
#         bin_html_file = os.path.join(html_dir, bin_id + '.html')
#         # write the html report to file
#         with open(bin_html_file, 'w') as html:
#
#             html.write('<html>\n')
#             html.write('<head>\n')
#             html.write('<title>CheckM Dist Plots for Bin ' + bin_id + '</title>')
#             html.write('<style style="text/css">\n a { color: #337ab7; } \n a:hover { color: #23527c; }\n</style>\n')
#             html.write('<body>\n')
#             html.write('<br><a href="CheckM_Table.html">Back to summary</a><br>\n')
#             html.write('<center><h2>Bin: ' + bin_id + '</h2></center>\n')
#             html.write('<img src="' + bin_id + self.PLOT_FILE_EXT + '" width="90%" />\n')
#             html.write('<br><br><br>\n')
#             html.write('</body>\n</html>\n')
#
#         return bin_html_file
#
#     def build_summary_tsv_file(self, bin_stats, removed_bins=None):
#
#         fields = self.get_fields()
#         run_config = self.run_config()
#
#         if not os.path.exists(run_config['tab_text_dir']):
#             os.makedirs(run_config['tab_text_dir'])
#
#         with open(run_config['tab_text_file'], 'w') as out_handle:
#
#             out_header = ['Bin Name']
#             for f in fields:
#                 out_header.append(f['display'])
#             if 'results_filtered' in run_config:
#                 out_header.append('QC Pass')
#
#             out_handle.write("\t".join(out_header)+"\n")
#
#             # DEBUG
#             #for bid in sorted(bin_stats.keys()):
#             #    log("BIN STATS BID: "+bid)
#
#             for bid in sorted(bin_stats.keys()):
#                 row = []
#                 row.append(bid)
#                 for f in fields:
#                     if f['id'] in bin_stats[bid]:
#                         value = str(bin_stats[bid][f['id']])
#                         if f.get('round'):
#                             value = str(round(bin_stats[bid][f['id']], f['round']))
#                         row.append(str(value))
#
#                 # add a column to indicate whether the bin should be removed
#                 if 'results_filtered' in run_config:
#                     if removed_bins:
#                         bin_id = re.sub('^[^\.]+\.', '', bid)
#                         if bin_id in removed_bins:
#                             row.append('false')
#                         else:
#                             row.append('true')
#                     else:
#                         row.append('true')
#
#                 out_handle.write("\t".join(row)+"\n")
#
#         return run_config['tab_text_file']
