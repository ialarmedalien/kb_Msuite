# -*- coding: utf-8 -*-
import os
import json
import shutil
from decimal import Decimal

from kb_Msuite.Utils.Utils import Base, LogMixin, TSVMixin
from kb_Msuite.Utils.FileUtils import (
    get_fasta_files,
    clean_up_bin_ID,
    copy_file_new_name_ignore_errors,
    read_bin_stats_file
)


class BinnedContigFilter(Base, LogMixin, TSVMixin):

    def __init__(self, checkMUtil_obj):
        self.checkMUtil = checkMUtil_obj

        for attr in ['client_util', 'workspacehelper', 'datastagingutils', 'outputbuilder']:
            setattr(self, attr, getattr(self.checkMUtil, attr))

    def client(self, client_name, *args):

        return self.client_util.client(client_name, *args)

    def run_config(self):

        return self.checkMUtil.run_config()

    def filter_binned_contigs(self, params, obj_info=None):

        run_config = self.run_config()

        obj_type = obj_info['obj_type'] if obj_info \
            else self.workspacehelper.get_ws_obj_type(params['input_ref'])

        self.logger.debug({'obj_type': obj_type})

        if obj_type == 'KBaseMetagenomes.BinnedContigs' \
          and 'output_filtered_binnedcontigs_obj_name' in params:
            run_config['results_filtered'] = True
        else:
            return None

        # these IDs are stripped of their suffixes
        fasta_files_by_bin_ID = get_fasta_files(
            run_config['input_dir'], run_config['fasta_ext']
        )
        if not fasta_files_by_bin_ID:
            return None

        self.logger.debug({'fasta_files_by_bin_ID': fasta_files_by_bin_ID})

        # fetch the existing binned_contig object
        binned_contig_obj = self.workspacehelper.get_obj_from_workspace(params['input_ref'])
        bin_summary_info = self.extract_binned_contigs_data(binned_contig_obj)
        bin_stats_raw_data = read_bin_stats_file(run_config['bin_stats_ext_file'])

        # read CheckM stats to get completeness and contamination scores
        test_completeness = False
        test_contamination = False
        if params.get('completeness_perc') and float(params.get('completeness_perc')) > 0.0:
            test_completeness = True
            completeness_thresh = float(params.get('completeness_perc'))
        if params.get('contamination_perc') and float(params.get('contamination_perc')) < 100.0:
            test_contamination = True
            contamination_thresh = float(params.get('contamination_perc'))

        fasta_ext_bc = run_config['fasta_ext_binned_contigs']
        filtered_bins_dir = run_config['filtered_bins_dir']
        if not os.path.exists(filtered_bins_dir):
            os.makedirs(filtered_bins_dir)

        bin_stats = dict()
        retained_bin_IDs = dict()
        removed_bin_IDs = dict()

        # write summary file for just those bins present in bin_dir
        summary_file = run_config['summary_file_path']
        self.logger.info(
            "writing filtered binned contigs summary file to " + summary_file
        )

        with open(summary_file, 'w', newline='') as summary_fh:
            summary_writer = self.init_write_summary_headers(summary_fh)

            # the fasta extension is stripped from the file in bin_stats
            for bin_ID in sorted(bin_stats_raw_data.keys()):

                # convert the raw data to JSON and reparse
                bin_stats[bin_ID] = json.loads(
                    json.dumps(bin_stats_raw_data[bin_ID]),
                    parse_float=Decimal
                )

                comp = float(bin_stats[bin_ID]['Completeness'])
                cont = float(bin_stats[bin_ID]['Contamination'])
                bin_is_HQ = True

                if test_completeness and comp < completeness_thresh:
                    bin_is_HQ = False
                    self.log_completeness_fail(bin_ID, comp, completeness_thresh)

                if test_contamination and cont > contamination_thresh:
                    bin_is_HQ = False
                    self.log_contamination_fail(bin_ID, cont, contamination_thresh)

                bin_stats[bin_ID]['QA Pass'] = bin_is_HQ

                self.logger.debug({'event': 'bin_stats_read', bin_ID: bin_stats[bin_ID]})

                if not bin_is_HQ:
                    self.logger.info(bin_ID + " didn't pass QC filters. Skipping.")
                    removed_bin_IDs[bin_ID] = True
                else:
                    self.logger.info(bin_ID + " passed QC filters. Adding to new BinnedContigs")
                    retained_bin_IDs[bin_ID] = True

                    # copy filtered file to filtered dir
                    new_file_name = bin_ID + '.' + fasta_ext_bc

                    src_path = fasta_files_by_bin_ID[bin_ID]
                    dst_path = os.path.join(filtered_bins_dir, new_file_name)

                    copy_file_new_name_ignore_errors(src_path, dst_path)

                    # write the row data to the summary file
                    if bin_ID in bin_summary_info:
                        summary_writer.writerow([
                            new_file_name,
                            bin_summary_info[bin_ID]['cov'],
                            bin_summary_info[bin_ID]['sum_contig_len'],
                            bin_summary_info[bin_ID]['gc'],
                        ])
                    else:
                        # wtf?
                        self.logger.error('No bin summary data found for ' + bin_ID)

        missing_ids = [bin_ID for bin_ID in fasta_files_by_bin_ID.keys()
                       if bin_ID not in bin_stats]
        if missing_ids:
            raise ValueError(
                "The following bin IDs are missing from the checkM output: "
                + ", ".join(sorted(missing_ids))
            )

        setattr(self.checkMUtil, 'bin_stats_data', bin_stats)
        # self.checkMUtil.bin_stats_data = bin_stats_data

        self.logger.debug({
            'retained_bin_IDs': retained_bin_IDs,
            'removed_bin_IDs':  removed_bin_IDs,
        })

        if not retained_bin_IDs or not removed_bin_IDs:
            if not removed_bin_IDs:
                self.logger.warning('No bins removed by filtering: no new BinnedContigs created.')
            # delete the filtered bins dir
            shutil.rmtree(filtered_bins_dir, ignore_errors=True)
            return None

        # create BinnedContig object from filtered bins
        new_binned_contigs_info = self.save_binned_contigs(
            params, binned_contig_obj['assembly_ref']
        )

        return {
            'filtered_obj_name': new_binned_contigs_info['obj_name'],
            'filtered_obj_ref':  new_binned_contigs_info['obj_ref'],
            'retained_bin_IDs':  retained_bin_IDs,
            'removed_bin_IDs':   removed_bin_IDs,
        }

    def log_contamination_fail(self, bin_ID, value, threshold):
        self.logger.info(
            bin_ID + ": contamination of " + str(value)
            + " above threshold " + str(threshold)
        )

    def log_completeness_fail(self, bin_ID, value, threshold):
        self.logger.info(
            bin_ID + ": completeness of " + str(value)
            + " below threshold " + str(threshold)
        )

    def init_write_summary_headers(self, summary_fh):

        summary_writer = self.init_tsv_writer(summary_fh)
        headers = ['Bin name', 'Completeness', 'Genome size', 'GC content']
        summary_writer.writerow(headers)
        return summary_writer

    def extract_binned_contigs_data(self, binned_contig_obj):

        run_config = self.run_config()
        fasta_ext = run_config['fasta_ext']
        bin_summary_info = {}

        # bin_item['bid'] is the full file name
        for bin_item in binned_contig_obj['bins']:
            bin_ID = clean_up_bin_ID(bin_item['bid'], fasta_ext)

            bin_summary_info[bin_ID] = {
                'cov':            str(round(100.0 * float(bin_item['cov']), 1)) + '%',
                'gc':             str(round(100.0 * float(bin_item['gc']), 1)),
                'sum_contig_len': str(bin_item['sum_contig_len']),
            }

            self.logger.debug({
                "bin_item[bid]": bin_item['bid'],
                'bin_ID':        bin_ID,
                'summary_info':  bin_summary_info[bin_ID],
            })

        return bin_summary_info

    def save_binned_contigs(self, params, assembly_ref):

        run_config = self.run_config()
        binned_contigs_ref = self.client('MetagenomeUtils', 'file_to_binned_contigs', {
            'file_directory':     run_config['filtered_bins_dir'],
            'assembly_ref':       assembly_ref,
            'binned_contig_name': params['output_filtered_binnedcontigs_obj_name'],
            'workspace_name':     params['workspace_name'],
        })

        return {
            'obj_name': params['output_filtered_binnedcontigs_obj_name'],
            'obj_ref':  binned_contigs_ref['binned_contig_obj_ref'],
        }
