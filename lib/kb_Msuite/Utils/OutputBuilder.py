import os
import shutil
import re
import ast
import sys
import time
import json

def log(message, prefix_newline=False):
    """Logging function, provides a hook to suppress or redirect log messages."""
    print(('\n' if prefix_newline else '') + '{0:.2f}'.format(time.time()) + ': ' + str(message))
    sys.stdout.flush()


class OutputBuilder(object):
    '''
    Constructs the output HTML report and artifacts based on a CheckM lineage_wf
    run.  This includes running any necssary plotting utilities of CheckM.
    '''

    # (self, run_config)
    def __init__(self, checkMUtil_obj):
        self.checkMUtil     = checkMUtil_obj
        self.client_util    = checkMUtil_obj.client_util
        self.scratch        = checkMUtil_obj.scratch
        self.DIST_PLOT_EXT  = '.ref_dist_plots.png'


    def client(self, client_name):

        return self.client_util.client(client_name)


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


    def package_folder(self, folder_path, zip_file_name, zip_file_description):
        ''' Simple utility for packaging a folder and saving to shock '''
        if folder_path == self.scratch:
            raise ValueError("cannot package folder that is not a subfolder of scratch")
        if not os.path.exists(folder_path):
            raise ValueError("cannot package folder that doesn't exist: " + folder_path)

        dfu = self.client('DataFileUtil')
        output = dfu.file_to_shock({
            'file_path': folder_path,
            'make_handle': 0,
            'pack': 'zip'
        })
        return {
            'shock_id': output['shock_id'],
            'name': zip_file_name,
            'description': zip_file_description,
        }


    def build_critical_output(self, critical_out_dir):

        run_config = self.checkMUtil.run_config()
        self._copy_file_ignore_errors('lineage.ms', run_config['output_dir'], critical_out_dir)

        storage_folder = os.path.join(critical_out_dir, 'storage')
        if not os.path.exists(storage_folder):
            os.makedirs(storage_folder)

        src = run_config['output_dir']
        dest = critical_out_dir
        self._copy_file_ignore_errors(os.path.join('storage', 'bin_stats.analyze.tsv'), src, dest)
        self._copy_file_ignore_errors(os.path.join('storage', 'bin_stats.tree.tsv'), src, dest)
        self._copy_file_ignore_errors(os.path.join('storage', 'bin_stats_ext.tsv'), src, dest)
        self._copy_file_ignore_errors(os.path.join('storage', 'marker_gene_stats.tsv'), src, dest)
        self._copy_file_ignore_errors(os.path.join('storage', 'tree', 'concatenated.tre'), src, dest)


    # requires the stats file - self.output_dir, 'storage', 'bin_stats_ext.tsv'
#    def build_html_output_for_lineage_wf(self, object_name, html_dir, results_filtered, removed_bins=None):
    def build_html_output_for_lineage_wf(self, bin_stats, removed_bins=None):

        '''
        Based on the output of CheckM lineage_wf, build an HTML report
        '''
        run_config = self.checkMUtil.run_config()
        html_dir    = run_config['html_dir']
        tmpl_src_dir    = run_config['template_src_dir']
        tmpl_dest_dir   = run_config['template_dest_dir']
        html_plots_dir  = os.path.join(html_dir, 'plots')

        os.makedirs(html_plots_dir)

        # move plots we need into the html directory
        self._copy_ref_dist_plots(html_plots_dir)

        # copy over the templates
        for tmpl in ['dist_html_page.tt', 'checkM_table.tt']:
            self._copy_file_ignore_errors(self, tmpl, tmpl_src_dir, tmpl_dest_dir)

        html_files = [
            {
                # checkm table:
                'template': {
                    'template_file': os.path.join(tmpl_dest_dir, 'checkM_table.tt'),
                },
                'name': 'checkm_results.html',
                'description': 'Summarized report from CheckM',
            },{
                'path': run_config['tab_text_file'],
                'name': run_config['tab_text_file_name'],
            },{
                'path': html_plots_dir,
                'name': 'plots',
            }
        ]

        for bid in sorted(bin_stats.keys()):
            # DEBUG
            bin_id = re.sub('^[^\.]+\.', '', bid)
            if removed_bins and bin_id in removed_bins:
                print("BIN STATS BID " + bid + ": REMOVED")
            else:
                print("BIN STATS BID " + bid)

            # create the dist plot page
            dist_plot_file = os.path.join(html_dir, str(bid) + self.DIST_PLOT_EXT)
            if os.path.isfile(dist_plot_file):
                html_files.append({
                    'template': {
                        'template_data_json': json.dumps({
                            'bin_id': bin_id,
                            'dist_plot_ext': self.DIST_PLOT_EXT,
                        }),
                        'template_file': os.path.join(tmpl_dest_dir, 'dist_html_page.tt'),
                    },
                    'name': bin_id + '.html',
                })

        return html_files

#         report_type = 'Table'
#
#         with open(html_file, 'w') as html:
#
#             html.write('<br><br><br>\n')
#             html.write('<div id="Summary" class="tabcontent">\n')
#             html.write('<table>\n')
#             html.write('  <tr>\n')
#             html.write('    <th><b>Bin Name</b></th>\n')
#
#             fields = self.get_fields()
#
#             for f in fields:
#                 html.write('    <th>' + f['display'] + '</th>\n')
#             html.write('  </tr>\n')
#
#             for bid in sorted(bin_stats.keys()):
#                 row_opening = '<tr>'
#                 if removed_bins:
#                     bin_id = re.sub('^[^\.]+\.', '', bid)
#                     if bin_id in removed_bins:
#                         row_bgcolor = '#F9E3E2'
#                         row_opening = '<tr style="background-color:'+row_bgcolor+'">'
#                 html.write('  '+row_opening+'\n')
#
#                 dist_plot_file = os.path.join(html_dir, str(bid) + self.DIST_PLOT_EXT)
#                 if os.path.isfile(dist_plot_file):
#
#                     self._write_dist_html_page(html_dir, bid)
#                     html.write('    <td><a href="' + bid + '.html">' + bid + '</td>\n')
#
#
#                 else:
#                     html.write('    <td>' + bid + '</td>\n')
#
#                 for f in fields:
#                     if f['id'] in bin_stats[bid]:
#                         value = str(bin_stats[bid][f['id']])
#                         if f.get('round'):
#                             value = str(round(bin_stats[bid][f['id']], f['round']))
#                         html.write('    <td>' + value + '</td>\n')
#                     else:
#                         html.write('    <td></td>\n')
#                 html.write('  </tr>\n')
#
#             html.write('</table>\n')
#             html.write('</div>\n')
#
#             html.write('</body>\n</html>\n')
#             html.close()

#        html_files.append(html_file)

#         html_zipped = self.package_folder(
#             html_dir,
#             html_files[0],
#             'Summarized report from CheckM')
#
#         return [html_zipped]


    def _write_dist_html_page(self, html_dir, bin_id):

        # write the html report to file
        with open(os.path.join(html_dir, bin_id + '.html'), 'w') as html:

            html.write('<html>\n')
            html.write('<head>\n')
            html.write('<title>CheckM Dist Plots for Bin' + bin_id + '</title>')
            html.write('<style style="text/css">\n a { color: #337ab7; } \n a:hover { color: #23527c; }\n</style>\n')
            html.write('<body>\n')
            html.write('<br><a href="CheckM_Table.html">Back to summary</a><br>\n')
            html.write('<center><h2>Bin: ' + bin_id + '</h2></center>\n')
            html.write('<img src="' + bin_id + self.DIST_PLOT_EXT + '" width="90%" />\n')
            html.write('<br><br><br>\n')
            html.write('</body>\n</html>\n')


    def read_bin_stats_file(self):
        run_config = self.checkMUtil.run_config()
        stats_file = run_config['bin_stats_ext_file']
        if not os.path.isfile(stats_file):
            log('Warning! no stats file found (looking at: ' + stats_file + ')')
            return

        bin_stats = dict()
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

    # 'tab_text/CheckM_summary_table.tsv'
#    def build_summary_tsv_file(self, run_config, tab_text_dir, tab_text_file, results_filtered, removed_bins=None):
    def build_summary_tsv_file(self, bin_stats, removed_bins=None):

        fields = self.get_fields()
        run_config = self.checkMUtil.run_config()

#        tab_text_files = []
        if not os.path.exists(run_config['tab_text_dir']):
            os.makedirs(run_config['tab_text_dir'])

        with open(run_config['tab_text_file'], 'w') as out_handle:

            out_header = ['Bin Name']
            for f in fields:
                out_header.append(f['display'])
            if 'results_filtered' in run_config:
                out_header.append('QC Pass')

            out_handle.write("\t".join(out_header)+"\n")

            # DEBUG
            #for bid in sorted(bin_stats.keys()):
            #    print ("BIN STATS BID: "+bid)

            for bid in sorted(bin_stats.keys()):
                row = []
                row.append(bid)
                for f in fields:
                    if f['id'] in bin_stats[bid]:
                        value = str(bin_stats[bid][f['id']])
                        if f.get('round'):
                            value = str(round(bin_stats[bid][f['id']], f['round']))
                        row.append(str(value))

                # add a column to indicate whether the bin should be removed
                if 'results_filtered' in run_config:
                    if removed_bins:
                        bin_id = re.sub('^[^\.]+\.', '', bid)
                        if bin_id in removed_bins:
                            row.append('false')
                        else:
                            row.append('true')
                    else:
                        row.append('true')

                out_handle.write("\t".join(row)+"\n")

#        return [tab_text_path]
        return run_config['tab_text_file']


    def _copy_file_ignore_errors(self, filename, src_folder, dest_folder):
        src = os.path.join(src_folder, filename)
        dest = os.path.join(dest_folder, filename)
        os.makedirs(dest_folder)
        log('copying ' + src + ' to ' + dest)
        try:
            shutil.copy(src, dest)
        except:
            # TODO: add error message reporting
            log('copy failed')


    def _copy_file_new_name_ignore_errors(self, src_path, dst_path):
        src = src_path
        dest = dst_path
        log('copying ' + src + ' to ' + dest)
        try:
            shutil.copy(src, dest)
        except:
            # TODO: add error message reporting
            log('copy failed')


    def build_report(self, removed_bins=None):

        return self.build_output_packages(removed_bins)

#    def build_output_packages(self, params, input_dir, removed_bins=None):
    def build_output_packages(self, removed_bins=None):

        output_packages = []
        run_config = self.checkMUtil.run_config()
        params = run_config['params']

        # create bin report summary TSV table text file
        log('creating TSV summary table text file')

        bin_stats_data = self.read_bin_stats_file()

        self.build_summary_tsv_file(bin_stats_data, removed_bins)

        tab_text_zipped = self.package_folder(
            run_config['tab_text_dir'],
            run_config['tab_text_file'] + '.zip',
            'TSV Summary Table from CheckM'
        )
        output_packages.append(tab_text_zipped)

        log('packaging full output directory')
        zipped_output_file = self.package_folder(
            run_config['output_dir'],
            'full_output.zip',
            'Full output of CheckM'
        )
        output_packages.append(zipped_output_file)

#         else:  # ADD LATER?
#             log('not packaging full output directory, selecting specific files')
#             crit_out_dir = os.path.join(self.scratch,
#                 'critical_output_' + os.path.basename(input_dir))
#             os.makedirs(crit_out_dir)
#             zipped_output_file = self.package_folder(self.output_dir,
#                 'selected_output.zip',
#                 'Selected output from the CheckM analysis')
#             output_packages.append(zipped_output_file)

        if 'save_plots_dir' in params and str(params['save_plots_dir']) == '1':
            log('packaging output plots directory')
            zipped_output_file = self.package_folder(
                run_config['plots_dir'],
                'plots.zip',
                'Output plots from CheckM')
            output_packages.append(zipped_output_file)
        else:
            log('not packaging output plots directory')

        # 6) build the HTML report
        html_files = self.build_html_output_for_lineage_wf(bin_stats_data, removed_bins)

        # 7) save report
#         report_params   = {
#             'message': '',
#             'direct_html_link_index': 0,
#             'html_links': html_files,
#             'file_links': output_packages,
#             'report_object_name': 'kb_checkM_report_' + str(uuid.uuid4()),
#             'workspace_name': params['workspace_name']
#         }

        return {
            'file_links': output_packages,
            'direct_html_link_index': 0,
            'html_links': html_files,
        }

    def _copy_ref_dist_plots(self, dest_folder):
        run_config = self.checkMUtil.run_config()
        plots_dir = run_config['plots_dir']
        for plotfile in os.listdir(plots_dir):
            plot_file_path = os.path.join(plots_dir, plotfile)
            if os.path.isfile(plot_file_path) and plotfile.endswith(self.DIST_PLOT_EXT):
                try:
                    shutil.copy(os.path.join(plots_dir, plotfile),
                                os.path.join(dest_folder, plotfile))
                except:
                    # TODO: add error message reporting
                    log('copy of ' + plot_file_path + ' to html directory failed')


    def save_binned_contigs(self, assembly_ref):
        run_config = self.checkMUtil.run_config()

        try:
            mgu = self.client('MetagenomeUtils')
        except:
            raise ValueError("unable to connect with MetagenomeUtils")

        object_name = run_config['params'].get('output_filtered_binnedcontigs_obj_name')
        params = {
            'file_directory':       run_config['filtered_bins_dir'],
            'assembly_ref':         assembly_ref,
            'binned_contig_name':   object_name,
            'workspace_name':       run_config['params'].get('workspace_name'),
        }
        object_ref = mgu.file_to_binned_contigs(params).get('binned_contig_obj_ref')

        return {
            'obj_name': object_name,
            'obj_ref':  object_ref,
        }


#    def build_bin_summary_file_from_binnedcontigs_obj(self, input_ref, bin_dir, bin_basename, fasta_extension):

    def build_bin_summary_file_from_binnedcontigs_obj(self, binned_contig_obj):

        run_config = self.checkMUtil.run_config()
        fasta_ext   = run_config['fasta_ext']
        bin_dir     = run_config['filtered_bins_dir']
        bin_basename = run_config['bin_basename']

        bin_summary_info = dict()
        # bid in object is full name of contig fasta file.  want just the number
        for bin_item in binned_contig_obj['bins']:
            #print ("BIN_ITEM[bid]: "+bin_item['bid'])  # DEBUG
            bin_ID = re.sub('^[^\.]+\.', '', bin_item['bid'].replace('.' + fasta_ext, ''))

            #print ("BIN_ID: "+bin_ID)  # DEBUG
            bin_summary_info[bin_ID] = {
                'n_contigs':        bin_item['n_contigs'],
                'gc':               round(100.0 * float(bin_item['gc']), 1),
                'sum_contig_len':   bin_item['sum_contig_len'],
                'cov':              round(100.0 * float(bin_item['cov']), 1),
            }

        # write summary file for just those bins present in bin_dir
        header_line = ['Bin name', 'Completeness', 'Genome size', 'GC content']

        dsu = self.checkMUtil.datastagingutils
        bin_fasta_files_by_bin_ID = dsu.get_bin_fasta_files(bin_dir, fasta_ext)
        bin_IDs = []
        for bin_ID in sorted(bin_fasta_files_by_bin_ID.keys()):
            bin_ID = re.sub('^[^\.]+\.', '', bin_ID.replace('.' + fasta_ext, ''))
            bin_IDs.append(bin_ID)

        summary_file_path = run_config['summary_file_path']

        print("writing filtered binned contigs summary file " + summary_file_path)
        with open(summary_file_path, 'w') as summary_file_handle:
            print("\t".join(header_line))
            summary_file_handle.write("\t".join(header_line)+"\n")
            for bin_ID in bin_IDs:
                #print ("EXAMINING BIN SUMMARY INFO FOR BIN_ID: "+bin_ID)  # DEBUG
                bin_summary_info_line = [
                    bin_basename + '.' + str(bin_ID) + '.' + fasta_ext,
                    str(bin_summary_info[bin_ID]['cov'])+'%',
                    str(bin_summary_info[bin_ID]['sum_contig_len']),
                    str(bin_summary_info[bin_ID]['gc'])
                ]
                print("\t".join(bin_summary_info_line))
                summary_file_handle.write("\t".join(bin_summary_info_line)+"\n")

        return summary_file_path
