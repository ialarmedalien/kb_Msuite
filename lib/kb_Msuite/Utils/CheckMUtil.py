# -*- coding: utf-8 -*-
import time
import os
import uuid
import subprocess
import sys
import re
import ast
import json
import logging
from decimal import Decimal

from kb_Msuite.Utils.DataStagingUtils import DataStagingUtils
from kb_Msuite.Utils.OutputBuilder import OutputBuilder
from kb_Msuite.Utils.ClientUtil import ClientUtil
from kb_Msuite.Utils.WorkspaceHelper import WorkspaceHelper
from kb_Msuite.Utils.Logger import Base, LogMixin

class CheckMUtil(Base, LogMixin):

    def __init__(self, config, ctx):
        self.config = config
        self.ctx = ctx

        self.client_util = ClientUtil({
            'callback_url':       config['SDK_CALLBACK_URL'],
            'service_wizard_url': config['srv-wiz-url'],
            'token':              ctx['token'],
            'workspace_url':      config['workspace-url'],
        })

        self.scratch = config['scratch']
        self.threads = config['threads']
        self.appdir  = config['appdir']
        self.fasta_extension = 'fna'
        self.binned_contigs_builder_fasta_extension = 'fasta'
        if not os.path.exists(self.scratch):
            os.makedirs(self.scratch)

        # init logger
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(name)s %(levelname)s %(message)s'
        )


    def client(self, client_name):
        return self.client_util.client(client_name)

    def run_config(self):
        if hasattr(self, '_run_config'):
            return self._run_config

        return self._set_run_config()

    def _set_run_config(self, params=None):

        suffix = str(int(time.time() * 1000))
        if params and 'dir_name' in params:
            suffix = '__' + params['dir_name'] + '__' + suffix

        base_dir = os.path.join(self.scratch, 'run_' + suffix)

        run_config = {
            'params': params,
            'suffix': suffix,
            'base_dir': base_dir,
            'input_dir': os.path.join(base_dir, 'bins'),
            'fasta_ext': self.fasta_extension,
            'fasta_ext_binned_contigs': self.binned_contigs_builder_fasta_extension,
            'results_filtered': False,  # this will be updated later
        }

        # directories
        for type in ['filtered_bins', 'output', 'plots', 'html']:
            run_config[type + '_dir'] = os.path.join(base_dir, type)

        tab_text_dir = os.path.join(base_dir, 'tab_text')
        run_config['tab_text_dir'] = tab_text_dir
        run_config['bin_basename'] = 'Bin'

        run_config['template_src_dir'] = os.path.join(self.appdir, 'templates')
        run_config['template_dest_dir'] = os.path.join(base_dir, 'templates')

        run_config['storage'] = os.path.join(run_config['output_dir'], 'storage')

        # files
        run_config['all_seq_fasta'] = os.path.join(base_dir, 'all_sequences.' + run_config['fasta_ext'])
        run_config['tetra_file'] = os.path.join(base_dir, 'tetra.tsv')

        run_config['bin_stats_ext_file'] = os.path.join(run_config['storage'], 'bin_stats_ext.tsv')

        run_config['tab_text_file_name'] = 'CheckM_summary_table.tsv'
        run_config['tab_text_file'] = os.path.join(tab_text_dir, run_config['tab_text_file_name'])

        run_config['html_file_name'] = 'checkm_results.html'
        run_config['html_file'] = os.path.join(run_config['html_dir'], run_config['html_file_name'])

        run_config['summary_file_path'] = os.path.join(
            run_config['filtered_bins_dir'], run_config['bin_basename'] + '.' + 'summary'
        )

        self._run_config = run_config
        self.datastagingutils   = DataStagingUtils(self)
        self.outputbuilder      = OutputBuilder(self)
        self.workspacehelper    = WorkspaceHelper(self)

        # self.logger.debug(run_config)

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

        # normalize boolean params
        for p in ['reduced_tree', 'save_plots_dir', 'save_output_dir']:
            if p in params:
                if params[p] is None or not int(params[p]) == 1:
                    del params[p]

        run_config = self._set_run_config(params)

        # 1) stage input data
        obj_info = self.datastagingutils.stage_input(params['input_ref'])
        self.logger.info('Staged input directory: ' + run_config['input_dir'])
        self.logger.info({'input object info': obj_info})

        # 2) run the lineage workflow
        lineage_wf_options = {
            'bin_folder': run_config['input_dir'],
            'out_folder': run_config['output_dir'],
            'threads':    self.threads,
        }
        if 'reduced_tree' in params:
            lineage_wf_options['reduced_tree'] = params['reduced_tree']

        # dump out the current dir structure
        self.run_checkM('lineage_wf', lineage_wf_options)

        # check whether it was successful
        if not os.path.exists(run_config['storage']):
            self.logger.warning('WARNING: no results found!')
            return self.outputbuilder.build_report(params)

        # 3) optionally filter bins by quality scores and save object
        filtered_obj_info = self._filter_binned_contigs(params, obj_info)

        # 4) make the plots:
        self.build_checkM_lineage_wf_plots()

        # 5) build the report and package output
        return self.outputbuilder.build_report(params, filtered_obj_info)

    def build_checkM_lineage_wf_plots(self):

        run_config = self.run_config()
        # compute tetranucleotide frequencies based on the concatenated fasta file
        self.logger.info('Computing tetranucleotide distributions...')
        tetra_options = {
            'seq_file':    run_config['all_seq_fasta'],
            'tetra_file':  run_config['tetra_file'],
            'threads':     self.threads,
            'quiet':       1
        }
        self.run_checkM('tetra', tetra_options) #, dropOutput=True)

        # plot distributions for each bin
        self.logger.info('Creating distribution plots per bin...')
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
        self.logger.debug('run_checkM: Running: ' + ' '.join(command) + '\n\n')
        run_config = self.run_config()

#         log_output_file = None
#         log_output_filename = None
#         if dropOutput:

        log_output_filename = os.path.join(run_config['base_dir'], subcommand + '.log')

        self.logger.debug('sending log output to ' + log_output_filename)
        with open(log_output_filename, 'w') as log_output_file:

            current_tree = subprocess.run(['tree', run_config['base_dir']],
                stdout=log_output_file, stderr=subprocess.STDOUT, universal_newlines=True)
            log_output_file.write(current_tree.stdout)
            log_output_file.write("\n\n\n")

            p = subprocess.Popen(command, cwd=self.scratch, shell=False,
                stdout=log_output_file, stderr=subprocess.STDOUT, universal_newlines=True)
    #         else:
    #             p = subprocess.Popen(command, cwd=self.scratch, shell=False)

            exitCode = p.wait()

            current_tree = subprocess.run(['tree', run_config['base_dir']],
                stdout=log_output_file, stderr=subprocess.STDOUT, universal_newlines=True)
            log_output_file.write("\n\n\n")
            log_output_file.write(current_tree.stdout)

#         if log_output_file:
#             log_output_file.close()

        if (exitCode == 0):
            self.logger.info('Executed command: ' + ' '.join(command) + '\n' +
                'Exit Code: ' + str(exitCode))
        else:
            self.logger.error('Error running command: ' + ' '.join(command) + '\n' + 'Logs:\n')
            with open(log_output_filename, 'r') as log_output_file:
                for line in log_output_file:
                    self.logger.error(line)

            raise ValueError('Error running command: ' + ' '.join(command) + '\n' +
                             'Exit Code: ' + str(exitCode))

    def _process_universal_options(self, command_list, options):
        if options.get('threads'):
            command_list.append('-t')
            command_list.append(str(options.get('threads')))

        if options.get('quiet') and str(options.get('quiet')) == '1':
            command_list.append('--quiet')

        return command_list

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
            raise ValueError('cannot run checkm ' + subcommand + ' without out_folder option set')
        if checkPlots and 'plots_folder' not in options:
            raise ValueError('cannot run checkm ' + subcommand + ' without plots_folder option set')
        if checkTetraFile and 'tetra_file' not in options:
            raise ValueError('cannot run checkm ' + subcommand + ' without tetra_file option set')

        return True

    def _build_command(self, subcommand, options):

        command = ['checkm', subcommand]
        self._process_universal_options(command, options)

        if subcommand == 'lineage_wf':
            self._validate_options(options, checkBin=True, checkOut=True, subcommand='lineage_wf')
            if 'reduced_tree' in options and str(options['reduced_tree']) == '1':
                command.append('--reduced_tree')
            command.append(options['bin_folder'])
            command.append(options['out_folder'])

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

    def _filter_binned_contigs(self, params):

        run_config = self.run_config()

#         return {
#             'obj_name': obj_name,
#             'obj_type': obj_type,
#         }

        obj_type = self.workspacehelper.get_data_obj_type(params['input_ref'])

        self.logger.debug('obj_type: ' + obj_type)

        if obj_type == 'KBaseMetagenomes.BinnedContigs' \
          and 'output_filtered_binnedcontigs_obj_name' in params:
            run_config['results_filtered'] = True
        else:
            return None

        bin_fasta_files_by_bin_ID = self.datastagingutils.get_bin_fasta_files(
            run_config['input_dir'], run_config['fasta_ext']
        )
        self.logger.debug({"bin_fasta_files_by_bin_ID": bin_fasta_files_by_bin_ID})

        if not bin_fasta_files_by_bin_ID:
            return None

        bin_IDs = sorted(bin_fasta_files_by_bin_ID.keys())

        self.logger.debug({"bin_IDs": bin_IDs})

        filtered_bins_dir = run_config['filtered_bins_dir']
        if not os.path.exists(filtered_bins_dir):
            os.makedirs(filtered_bins_dir)

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

        bin_stats_data = dict()
        retained_bin_IDs = dict()
        removed_bin_IDs = dict()

        bin_stats_ext_file = run_config['bin_stats_ext_file']
        bin_basename = run_config['bin_basename']

        file_ext = run_config['fasta_ext_binned_contigs']
        some_bins_are_HQ = False

        bin_stats = self.outputbuilder.read_bin_stats_file()

#     def read_bin_stats_file(self):
#         run_config = self.run_config()
#         stats_file = run_config['bin_stats_ext_file']
#
#         bin_stats = dict()
#
#         if not os.path.isfile(stats_file):
#             log('Warning! no stats file found (looking at: ' + stats_file + ')')
#             return bin_stats
#
#         with open(stats_file) as lf:
#             for line in lf:
#                 if not line:
#                     continue
#                 if line.startswith('#'):
#                     continue
#                 col = line.split('\t')
#                 bin_id = str(col[0])
#                 data = ast.literal_eval(col[1])
#                 bin_stats[bin_id] = data
#
#         return bin_stats


        with open(bin_stats_ext_file, 'r') as bin_stats_ext_handle:
            for bin_stats_line in bin_stats_ext_handle:
                bin_stats_line.rstrip()
                [full_bin_ID, bin_stats_json_str] = bin_stats_line.split("\t")
                bin_ID = self.clean_bin_ID(full_bin_ID, run_config['fasta_ext'])

#                bin_stats_json_str = json.dumps(ast.literal_eval(bin_stats_json_str))
                bin_stats_data[bin_ID] = json.loads(
                    json.dumps(ast.literal_eval(bin_stats_json_str)),
                    parse_float=Decimal
                )

                comp = float(bin_stats_data[bin_ID]['Completeness'])
                cont = float(bin_stats_data[bin_ID]['Contamination'])

                self.logger.debug("Bin " + bin_ID + " CheckM COMPLETENESS:  " + str(comp))
                self.logger.debug("Bin " + bin_ID + " CheckM CONTAMINATION: " + str(cont))

                bin_is_HQ = True
                if test_completeness and comp < completeness_thresh:
                    bin_is_HQ = False
                    self.logger.info("Bin " + bin_ID + " Completeness of " + str(comp) + " below thresh " + str(completeness_thresh))
                if test_contamination and cont > contamination_thresh:
                    bin_is_HQ = False
                    self.logger.info("Bin " + bin_ID + " Contamination of " + str(cont) + " above thresh " + str(contamination_thresh))

                if not bin_is_HQ:
                    self.logger.info("Bin " + bin_ID + " didn't pass QC filters.  Skipping.")
                    removed_bin_IDs[bin_ID] = True
                else:
                    self.logger.info("Bin " + bin_ID + " passed QC filters.  Adding to new BinnedContigs")
                    some_bins_are_HQ = True
                    retained_bin_IDs[bin_ID] = True

                    # copy filtered bin scaffold files to filtered dir
                    src_path = bin_fasta_files_by_bin_ID[bin_ID]
                    dst_path = os.path.join(filtered_bins_dir,
                        bin_basename + '.' + str(bin_ID) + '.' + file_ext)
                    self.outputbuilder._copy_file_new_name_ignore_errors(src_path, dst_path)

        missing_ids = [bin_ID for bin_ID in bin_IDs if bin_ID not in bin_stats_data]
        self.logger.info({"missing IDs:": missing_ids})
        if missing_ids:
            raise ValueError("The following Bin IDs are missing from the checkM output: "
                + ", ".join(sorted(missing_ids)))

        self.bin_stats_data = bin_stats_data

        if not some_bins_are_HQ:
            return None

        # create BinnedContig object from filtered bins
        binned_contig_obj = self.workspacehelper.get_obj_from_workspace(params['input_ref'])
        self.build_bin_summary_file_from_binnedcontigs_obj(binned_contig_obj, retained_bin_IDs)
        new_binned_contigs_info = self.save_binned_contigs(params, binned_contig_obj['assembly_ref'])

        return {
            'filtered_obj_name': new_binned_contigs_info['obj_name'],
            'filtered_obj_ref':  new_binned_contigs_info['obj_ref'],
            'retained_bin_IDs':  retained_bin_IDs,
            'removed_bin_IDs':   removed_bin_IDs,
        }

    def build_bin_summary_file_from_binnedcontigs_obj(self, binned_contig_obj, retained_bin_IDs):

        run_config   = self.run_config()
        fasta_ext    = run_config['fasta_ext']
        bin_dir      = run_config['filtered_bins_dir']
        bin_basename = run_config['bin_basename']

        dsu = self.datastagingutils
        filtered_bin_ID_dict = dsu.get_bin_fasta_files(run_config['filtered_bins_dir'], fasta_ext)

        filtered_bin_IDs = [self.clean_bin_ID(bin_ID, fasta_ext) for bin_ID in sorted(filtered_bin_ID_dict.keys())]

        self.logger.debug('filtered_bin_IDs:')
        self.logger.debug(filtered_bin_IDs)

        self.logger.debug('retained_bin_IDs')
        self.logger.debug(retained_bin_IDs)

        bin_summary_info = dict()

        # bid in object is full name of contig fasta file. want just the number
        for bin_item in binned_contig_obj['bins']:
            bin_ID = self.clean_bin_ID(bin_item['bid'], fasta_ext)

            #self.logger.debug("BIN_ITEM[bid]: "+bin_item['bid'])  # DEBUG
            #self.logger.debug("BIN_ID: "+bin_ID)  # DEBUG
            bin_summary_info[bin_ID] = {
                'n_contigs':        bin_item['n_contigs'],
                'gc':               round(100.0 * float(bin_item['gc']), 1),
                'sum_contig_len':   bin_item['sum_contig_len'],
                'cov':              round(100.0 * float(bin_item['cov']), 1),
            }

        # write summary file for just those bins present in bin_dir
        self.logger.info("writing filtered binned contigs summary file " + summary_file_path)

        summary_file_path = run_config['summary_file_path']
        with open(summary_file_path, 'w') as summary_file_handle:

            header_line = ['Bin name', 'Completeness', 'Genome size', 'GC content']
            summary_file_handle.write("\t".join(header_line)+"\n")


            for bin_ID in filtered_bin_IDs:
                #self.logger.debug("EXAMINING BIN SUMMARY INFO FOR BIN_ID: "+bin_ID)  # DEBUG
                bin_summary_info_line = [
                    bin_basename + '.' + str(bin_ID) + '.' + fasta_ext,
                    str(bin_summary_info[bin_ID]['cov'])+'%',
                    str(bin_summary_info[bin_ID]['sum_contig_len']),
                    str(bin_summary_info[bin_ID]['gc'])
                ]
                summary_file_handle.write("\t".join(bin_summary_info_line)+"\n")

        return summary_file_path

    def save_binned_contigs(self, params, assembly_ref):

        run_config   = self.run_config()
        binned_contigs_ref = self.client('MetagenomeUtils').file_to_binned_contigs({
            'file_directory':       run_config['filtered_bins_dir'],
            'assembly_ref':         assembly_ref,
            'binned_contig_name':   params['output_filtered_binnedcontigs_obj_name'],
            'workspace_name':       params['workspace_name'],
        })

        return {
            'obj_name': params['output_filtered_binnedcontigs_obj_name'],
            'obj_ref':  binned_contigs_ref['binned_contig_obj_ref'],
        }

    def clean_bin_ID(self, bin_id, extension=''):

        return re.sub('^[^\.]+\.', '', bin_id.replace('.' + extension, ''))
