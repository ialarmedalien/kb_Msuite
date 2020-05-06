# -*- coding: utf-8 -*-
import time
import os
import subprocess
import logging

from kb_Msuite.Utils.DataStagingUtils import DataStagingUtils
from kb_Msuite.Utils.OutputBuilder import OutputBuilder
from kb_Msuite.Utils.ClientUtil import ClientUtil
from kb_Msuite.Utils.WorkspaceHelper import WorkspaceHelper
from kb_Msuite.Utils.BinnedContigFilter import BinnedContigFilter
from kb_Msuite.Utils.Utils import Base, LogMixin


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
        self.appdir = config['appdir']
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
        run_config['bin_basename'] = 'bin'

        run_config['template_src_dir'] = os.path.join(self.appdir, 'templates')
        run_config['template_dest_dir'] = os.path.join(base_dir, 'templates')

        run_config['storage'] = os.path.join(run_config['output_dir'], 'storage')

        # files
        run_config['all_seq_fasta'] = os.path.join(
            base_dir, 'all_sequences.' + run_config['fasta_ext']
        )
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
        self.workspacehelper = WorkspaceHelper(self)
        self.datastagingutils = DataStagingUtils(self)
        self.outputbuilder = OutputBuilder(self)
        self.binnedcontigfilter = BinnedContigFilter(self)

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
        self.logger.debug({'input object info': obj_info})

        # 2) run the lineage workflow
        lineage_wf_options = {
            'bin_folder': run_config['input_dir'],
            'out_folder': run_config['output_dir'],
            'threads':    self.threads,
        }
        if 'reduced_tree' in params:
            lineage_wf_options['reduced_tree'] = params['reduced_tree']

        self.run_checkM('lineage_wf', lineage_wf_options)

        # check whether it was successful
        if not os.path.exists(run_config['storage']):
            self.logger.warning('WARNING: no results found!')
            return self.outputbuilder.build_report(params)

        # 3) optionally filter bins by quality scores and save object
        filtered_obj_info = self.binnedcontigfilter.filter_binned_contigs(params, obj_info)

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
            'quiet':       1,
        }
        self.run_checkM('tetra', tetra_options)

        # plot distributions for each bin
        self.logger.info('Creating distribution plots per bin...')
        dist_plot_options = {
            'bin_folder':   run_config['input_dir'],
            'out_folder':   run_config['output_dir'],
            'plots_folder': run_config['plots_dir'],
            'tetra_file':   run_config['tetra_file'],
            'dist_value':   95,
            'quiet':        1,
        }
        self.run_checkM('dist_plot', dist_plot_options)

    def run_checkM(self, subcommand, options):
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
        run_config = self.run_config()
        log_dir = os.path.join(run_config['base_dir'], 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_output_filename = os.path.join(run_config['base_dir'], 'logs', subcommand + '.log')

        self.logger.debug('run_checkM: Running: ' + ' '.join(command))
        self.logger.debug('sending log output to ' + log_output_filename)

        with open(log_output_filename, 'w') as log_output_file:

            p = subprocess.Popen(
                command, cwd=self.scratch, shell=False,
                stdout=log_output_file, stderr=subprocess.STDOUT, universal_newlines=True)

            exitCode = p.wait()

        self.logger.info('Executed command: ' + ' '.join(command))
        self.logger.info('Exit Code: ' + str(exitCode))

        if (exitCode != 0):
            self.logger.error('Error running command: ' + ' '.join(command) + '\n' + 'Logs:\n')
            with open(log_output_filename, 'r') as log_output_file:
                for line in log_output_file:
                    self.logger.error(line)

            raise ValueError('Stopped execution due to exit code ' + str(exitCode))

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
            if 'reduced_tree' in options:
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
            if 'dist_value' not in options:
                raise ValueError('cannot run checkm dist_plot without dist_value option set')
            command.append(options['out_folder'])
            command.append(options['bin_folder'])
            command.append(options['plots_folder'])
            command.append(options['tetra_file'])
            command.append(str(options['dist_value']))

        else:
            raise ValueError('Invalid or unsupported checkM subcommand: ' + str(subcommand))

        return command
