# -*- coding: utf-8 -*-
import time
import os
import uuid
import subprocess
import sys
import re
import ast
import json
from decimal import Decimal

from installed_clients.KBaseReportClient import KBaseReport

from kb_Msuite.Utils.DataStagingUtils import DataStagingUtils
from kb_Msuite.Utils.OutputBuilder import OutputBuilder


def log(message, prefix_newline=False):
    """Logging function, provides a hook to suppress or redirect log messages."""
    print(('\n' if prefix_newline else '') + '{0:.2f}'.format(time.time()) + ': ' + str(message))
    sys.stdout.flush()


class CheckMUtil:

    def __init__(self, config, ctx):
        self.config = config
        self.ctx = ctx
        self.callback_url = config['SDK_CALLBACK_URL']
        self.scratch = config['scratch']
        self.threads = config['threads']
        self.fasta_extension = 'fna'
        self.binned_contigs_builder_fasta_extension = 'fasta'
        if not os.path.exists(self.scratch):
            os.makedirs(self.scratch)


    def set_run_configuration(self, params):

        suffix = str(int(time.time() * 1000))
        base_dir = os.path.join(self.scratch,'run_' + suffix)

        run_config = {
            'params': params,
            'suffix': suffix,
            'base_dir': base_dir,
            'input_dir': os.path.join(base_dir, 'bins'),
            'fasta_ext': self.fasta_extension,
            'fasta_ext_binned_contigs': self.binned_contigs_builder_fasta_extension,
            'results_filtered': False, # this will be updated later
        }

        # directories
        for type in ['filtered_bins', 'output', 'plots', 'html']:
            run_config[type + '_dir'] = os.path.join(base_dir, type)

        tab_text_dir = os.path.join(run_config['output_dir'], 'tab_text')
        run_config['tab_text_dir'] = tab_text_dir

        run_config['bin_basename'] = 'Bin'

        # files
        run_config['all_seq_fasta'] = os.path.join(base_dir,
            'all_sequences.' + run_config['fasta_ext']
        )
        run_config['tetra_file'] = os.path.join(base_dir, 'tetra.tsv')

        run_config['bin_stats_ext_file'] = os.path.join(
            run_config['output_dir'], 'storage', 'bin_stats_ext.tsv'
        )

        run_config['tab_text_file_name'] = 'CheckM_summary_table.tsv'
        run_config['tab_text_file'] = os.path.join(tab_text_dir, run_config['tab_text_file_name'])


        run_config['html_file_name'] = 'checkm_results.html'
        run_config['html_file'] = os.path.join(run_config['html_dir'], run_config['html_file_name'])

        summary_file_path = os.path.join(
            run_config['filtered_bins_dir'], run_config['bin_basename'] + '.' + 'summary'
        )
        self.run_config = run_config

        self.datastagingutils   = DataStagingUtils(self)
        self.outputbuilder      = OutputBuilder(self)

        return run_config

    def run_checkM_lineage_wf(self, params):
        '''
        Main entry point for running the lineage_wf as a KBase App
        '''

        # 0) validate basic parameters
        if 'input_ref' not in params:
            raise ValueError('input_ref field was not set in params for run_checkM_lineage_wf')
        if 'workspace_name' not in params:
            raise ValueError('workspace_name field was not set in params for run_checkM_lineage_wf')

        if 'reduced_tree' in params:
            if params['reduced_tree'] is not None and int(params['reduced_tree'])) == 1:
                # fine
            else:
                del params['reduced_tree']

        run_config = self.set_run_configuration(params)

        # 1) stage input data
        self.datastagingutils.stage_input(params['input_ref'])
        log('Staged input directory: ' + run_config['input_dir'])

        # 2) run the lineage workflow
        lineage_wf_options = {
            'bin_folder': run_config['input_dir'],
            'out_folder': run_config['output_dir'],
            'threads':    self.threads,
        }

        if ('reduced_tree' in params):
            lineage_wf_options['reduced_tree'] = params['reduced_tree']

        self.run_checkM('lineage_wf', lineage_wf_options)


        # 3) optionally filter bins by quality scores and save object
        binned_contig_obj_ref = None
        created_objects = None
        removed_bins = None


#         if self.datastagingutils.get_data_obj_type(params['input_ref']) == 'KBaseMetagenomes.BinnedContigs' \
#           and params.get('output_filtered_binnedcontigs_obj_name'):
#             filter_results = True
#             self.run_config['results_filtered'] = True
#             self.run_config = run_config
        filtered_obj_info = self._filter_binned_contigs()
#                                                             input_dir,
#                                                             output_dir,
#                                                             filtered_bins_dir)
        if filtered_obj_info is None:
            log("No Bins passed QC filters.  Not saving filtered BinnedContig object")
        else:
            binned_contig_obj_ref   = filtered_obj_info['filtered_obj_ref']
            removed_bins            = filtered_obj_info['removed_bin_IDs']
            created_objects = [{
                'ref':          binned_contig_obj_ref,
                'description':  'HQ BinnedContigs ' + filtered_obj_info['filtered_obj_name']
            }]

        # 4) make the plots:
        self.build_checkM_lineage_wf_plots()
#            input_dir, output_dir, plots_dir, all_seq_fasta_file, tetra_file)


        # 5) Package results
        #   includes building the .tsv file that the HTML report is built from
        output_packages = self.outputbuilder.build_output_packages(removed_bins)
#             params,
#             input_dir,
#             filter_results,
#             removed_bins=removed_bins)

        # 6) build the HTML report
        html_files = self.outputbuilder.build_html_output_for_lineage_wf(removed_bins)
#             params['input_ref'],
#             html_dir,
#             filter_results,
#             removed_bins=removed_bins)

        html_zipped = self.outputbuilder.package_folder(
            run_config['html_dir'],
            html_files[0],
            'Summarized report from CheckM')

        # 7) save report
        report_params = {
            'message': '',
             'direct_html_link_index': 0,
             'html_links': [html_zipped],
             'file_links': output_packages,
             'report_object_name': 'kb_checkM_report_' + str(uuid.uuid4()),
             'workspace_name': params['workspace_name']
        }

        if created_objects:
            report_params['objects_created'] = created_objects

        kr = KBaseReport(self.callback_url)
        report_output = kr.create_extended_report(report_params)

        returnVal = {
            'report_name': report_output['name'],
            'report_ref':  report_output['ref'],
        }

        if binned_contig_obj_ref:
            returnVal.update({'binned_contig_obj_ref': binned_contig_obj_ref})

        return returnVal


    def build_checkM_lineage_wf_plots(self):
#            input_dir, output_dir, plots_dir, all_seq_fasta_file, tetra_file)

#     bin_folder, out_folder, plots_folder,
#                                       all_seq_fasta_file, tetra_file):

        # first build generic plot for entire dataset
#         log('Creating basic QA plot (checkm bin_qa_plot) ...')
#         bin_qa_plot_options = {'bin_folder': bin_folder,
#                                'out_folder': out_folder,
#                                'plots_folder': plots_folder
#                                }
#         self.run_checkM('bin_qa_plot', bin_qa_plot_options, dropOutput=True)
        run_config = self.run_config
        # compute tetranucleotide frequencies based on the concatenated fasta file
        log('Computing tetranucleotide distributions...')
        tetra_options = {
            'seq_file':    run_config['all_seq_fasta'],
            'tetra_file':  run_config['tetra_file'],
            'threads':     self.threads,
            'quiet':       1
        }
        self.run_checkM('tetra', tetra_options) #, dropOutput=True)

        # plot distributions for each bin
        log('Creating distribution plots per bin...')
        dist_plot_options = {
            'bin_folder':   run_config['input_dir'],
            'out_folder':   run_config['output_dir'],
            'plots_folder': run_config['plots_dir'],
            'tetra_file':   run_config['tetra_file'],
            'dist_value':   95,
            'quiet':        1
        }
        self.run_checkM('dist_plot', dist_plot_options) #, dropOutput=True)


    def run_checkM(self, subcommand, options): #, dropOutput=True):
        '''
            subcommand is the checkm subcommand (eg lineage_wf, tetra, etc)
            options indicate, depending on the subcommand:
                bin_folder
                out_folder
                plots_folder
                seq_file
                tetra_file
                reduced_tree
                threads
                dist_value
        '''
        command = self._build_command(subcommand, options)
        log('\n\ncheckMUtil.run_checkM: Running: ' + ' '.join(command) + '\n\n')
        run_config = self.run_config

#         log_output_file = None
#         log_output_filename = None
#         if dropOutput:

        log_file_extra = os.environ.get( 'KB_TEST_ID', str(int(time.time() * 1000)) )
        log_output_filename = os.path.join(run_config['base_dir'], subcommand + '--' + log_file_extra + '.log')


        log('sending log output to ' + log_output_filename)
        log_output_file = open(log_output_filename, 'w')
        p = subprocess.Popen(command, cwd=self.scratch, shell=False,
                             stdout=log_output_file, stderr=subprocess.STDOUT)
#         else:
#             p = subprocess.Popen(command, cwd=self.scratch, shell=False)

        exitCode = p.wait()

        if log_output_file:
            log_output_file.close()

        if (exitCode == 0):
            log('Executed command: ' + ' '.join(command) + '\n' +
                'Exit Code: ' + str(exitCode))
        else:
            log('Error running command: ' + ' '.join(command) + '\n' + 'Logs:\n')
            log_output_file = open(log_output_filename, 'r')
            for line in log_output_file:
                log(line)
            raise ValueError('Error running command: ' + ' '.join(command) + '\n' +
                             'Exit Code: ' + str(exitCode))


    def _process_universal_options(self, command_list, options):
        if options.get('threads'):
            command_list.append('-t')
            command_list.append(str(options.get('threads')))

        if options.get('quiet') and str(options.get('quiet')) == '1':
            command_list.append('--quiet')


    def _validate_options(self, options,
                          checkBin=False,
                          checkOut=False,
                          checkPlots=False,
                          checkTetraFile=False,
                          subcommand=''):
        # Note: we can, maybe should, add additional checks on the contents of the folders here
        if checkBin and 'bin_folder' not in options:
            raise ValueError('cannot run checkm ' + subcommand + ' without bin_folder option set')
        if checkOut and 'out_folder' not in options:
            raise ValueError('cannot run checkm ' + subcommand + ' without bin_folder option set')
        if checkPlots and 'plots_folder' not in options:
            raise ValueError('cannot run checkm ' + subcommand + ' without plots_folder option set')
        if checkTetraFile and 'tetra_file' not in options:
            raise ValueError('cannot run checkm ' + subcommand + ' without tetra_file option set')


    def _build_command(self, subcommand, options):

        command = ['checkm', subcommand]
        self._process_universal_options(command, options)

        if subcommand == 'lineage_wf':
            self._validate_options(options, checkBin=True, checkOut=True, subcommand='lineage_wf')
            if 'reduced_tree' in options and str(options['reduced_tree']) == '1':
                command.append('--reduced_tree')
            command.append(options['bin_folder'])
            command.append(options['out_folder'])

#         elif subcommand == 'bin_qa_plot':
#             self._validate_options(options, checkBin=True, checkOut=True, checkPlots=True,
#                                    subcommand='bin_qa_plot')
#             command.append(options['out_folder'])
#             command.append(options['bin_folder'])
#             command.append(options['plots_folder'])

        elif subcommand == 'tetra':
            self._validate_options(options, checkTetraFile=True, subcommand='tetra')
            if 'seq_file' not in options:
                raise ValueError('cannot run checkm tetra without seq_file option set')
            command.append(options['seq_file'])
            command.append(options['tetra_file'])

        elif subcommand == 'dist_plot':
            self._validate_options(options, checkBin=True, checkOut=True, checkPlots=True,
                                   checkTetraFile=True, subcommand='dist_plot')
            command.append(options['out_folder'])
            command.append(options['bin_folder'])
            command.append(options['plots_folder'])
            command.append(options['tetra_file'])
            if 'dist_value' not in options:
                raise ValueError('cannot run checkm dist_plot without dist_value option set')
            command.append(str(options['dist_value']))

        else:
            raise ValueError('Invalid or unsupported checkM subcommand: ' + str(subcommand))

        return command


    def _filter_binned_contigs(self):

        if self.datastagingutils.get_data_obj_type(self.run_config.params['input_ref']) == 'KBaseMetagenomes.BinnedContigs' \
          and self.run_config.params.get('output_filtered_binnedcontigs_obj_name'):
            self.run_config['results_filtered'] = True
        else:
            return

        run_config          = self.run_config
        input_dir           = run_config['input_dir']
        output_dir          = run_config['output_dir']
        filtered_bins_dir   = run_config['filtered_bins_dir']

        # prep fs stuff and get bin IDs
        if not os.path.exists(filtered_bins_dir):
            os.makedirs(filtered_bins_dir)

        bin_fasta_files_by_bin_ID = self.datastagingutils.get_bin_fasta_files(
            input_dir, run_config['fasta_ext'])
#            input_dir, self.fasta_extension)

        log("DEBUG: bin_fasta_files_by_bin_ID: ")
        log(bin_fasta_files_by_bin_ID)

        bin_IDs = []
        for bin_ID in sorted(bin_fasta_files_by_bin_ID.keys()):
            bin_IDs.append(bin_ID)
            log("Contigs Fasta file found for Bin ID: " + bin_ID)

        # read CheckM stats to get completeness and contamination scores
        test_completeness = False
        test_contamination = False
        if params.get('completeness_perc') \
           and float(params.get('completeness_perc')) > 0.0:
            test_completeness = True
            completeness_thresh = float(params.get('completeness_perc'))
        if params.get('contamination_perc') \
           and float(params.get('contamination_perc')) < 100.0:
            test_contamination = True
            contamination_thresh = float(params.get('contamination_perc'))

        bin_stats_obj = dict()
        retained_bin_IDs = dict()
        removed_bin_IDs = dict()

        bin_stats_ext_file = run_config['bin_stats_ext_file']
        bin_basename = run_config['bin_basename']

        file_ext = run_config['fasta_ext_binned_contigs']
        some_bins_are_HQ = False

        with open(bin_stats_ext_file, 'r') as bin_stats_ext_handle:
            for bin_stats_line in bin_stats_ext_handle:
                bin_stats_line.rstrip()
                [full_bin_ID, bin_stats_json_str] = bin_stats_line.split("\t")
                bin_ID = re.sub('^[^\.]+\.', '', full_bin_ID.replace(run_config['fasta_ext'], ''))

                bin_stats_json_str = json.dumps(ast.literal_eval(bin_stats_json_str))
                bin_stats_obj[bin_ID] = json.loads(bin_stats_json_str, parse_float=Decimal)

                comp = float(bin_stats_obj[bin_ID]['Completeness'])
                cont = float(bin_stats_obj[bin_ID]['Contamination'])

                log("Bin " + bin_ID + " CheckM COMPLETENESS:  " + str(comp))
                log("Bin " + bin_ID + " CheckM CONTAMINATION: " + str(cont))

                bin_is_HQ = True
                if test_completeness and comp < completeness_thresh:
                    bin_is_HQ = False
                    log("Bin " + bin_ID + " Completeness of " + str(comp) + " below thresh " + str(completeness_thresh))
                if test_contamination and cont > contamination_thresh:
                    bin_is_HQ = False
                    log("Bin " + bin_ID + " Contamination of " + str(cont) + " above thresh " + str(contamination_thresh))

                if not bin_is_HQ:
                    log("Bin " + bin_ID + " didn't pass QC filters.  Skipping.")
                    removed_bin_IDs[bin_ID] = True
                else:
                    log("Bin " + bin_ID + " passed QC filters.  Adding to new BinnedContigs")
                    some_bins_are_HQ = True
                    retained_bin_IDs[bin_ID] = True

                    # copy filtered bin scaffold files to filtered dir
                    src_path = bin_fasta_files_by_bin_ID[bin_ID]
                    dst_path = os.path.join(filtered_bins_dir,
                        bin_basename + '.' + str(bin_ID) + '.' + file_ext)
                    self.outputbuilder._copy_file_new_name_ignore_errors(src_path, dst_path)

        for bin_ID in bin_IDs:
            if bin_ID not in bin_stats_obj:
                raise ValueError("Bin ID " + bin_ID + " not found in bin stats")

#             QC_scores[bin_ID] = dict()
#             QC_scores[bin_ID]['completeness'] = float(bin_stats_obj[bin_ID]['Completeness'])
#             QC_scores[bin_ID]['contamination'] = float(bin_stats_obj[bin_ID]['Contamination'])

#        for bin_ID in bin_IDs:

#         for bin_ID in bin_IDs:
#             if bin_ID not in retained_bin_IDs:
#                 removed_bin_IDs[bin_ID] = True

        if not some_bins_are_HQ:
            return None

        # create BinnedContig object from filtered bins
        binned_contig_obj = self.datastagingutils.get_obj_from_workspace(params['input_ref'])

        summary_file = self.outputbuilder.build_bin_summary_file_from_binnedcontigs_obj(
            binned_contig_obj
        )

        assembly_ref = binned_contig_obj['assembly_ref']
        new_binned_contigs_info = self.outputbuilder.save_binned_contigs(assembly_ref)

        return {
            'filtered_obj_name': new_binned_contigs_info['obj_name'],
            'filtered_obj_ref':  new_binned_contigs_info['obj_ref'],
            'retained_bin_IDs':  retained_bin_IDs,
            'removed_bin_IDs':   removed_bin_IDs,
        }
