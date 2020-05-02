# -*- coding: utf-8 -*-
import os
import json
import shutil
from decimal import Decimal

from kb_Msuite.Utils.Utils import Base, LogMixin, TSVMixin


class BinnedContigFilter(Base, LogMixin, TSVMixin):

    def __init__(self, checkMUtil_obj):
        self.checkMUtil = checkMUtil_obj

        for attr in ['client_util', 'workspacehelper', 'datastagingutils', 'outputbuilder']:
            setattr(self, attr, getattr(self.checkMUtil, attr))

    def client(self, client_name):

        return self.client_util.client(client_name)

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

        bin_fasta_files_by_bin_ID = self.datastagingutils.get_bin_fasta_files(
            run_config['input_dir'], run_config['fasta_ext']
        )
        if not bin_fasta_files_by_bin_ID:
            return None

        bin_IDs = sorted(bin_fasta_files_by_bin_ID.keys())
        self.logger.debug({"bin_IDs": bin_IDs})

        # fetch the existing binned_contig object
        binned_contig_obj = self.workspacehelper.get_obj_from_workspace(params['input_ref'])
        bin_summary_info = self.extract_binned_contigs_data(binned_contig_obj)
        bin_stats_raw_data = self.checkMUtil.read_bin_stats_file()

        # read CheckM stats to get completeness and contamination scores
        test_completeness = False
        test_contamination = False
        if params.get('completeness_perc') and float(params.get('completeness_perc')) > 0.0:
            test_completeness = True
            completeness_thresh = float(params.get('completeness_perc'))
        if params.get('contamination_perc') and float(params.get('contamination_perc')) < 100.0:
            test_contamination = True
            contamination_thresh = float(params.get('contamination_perc'))

        bin_basename = run_config['bin_basename']
        fasta_ext_bc = run_config['fasta_ext_binned_contigs']
        filtered_bins_dir = run_config['filtered_bins_dir']
        if not os.path.exists(filtered_bins_dir):
            os.makedirs(filtered_bins_dir)

        bin_stats_data = dict()
        retained_bin_IDs = dict()
        removed_bin_IDs = dict()

        # write summary file for just those bins present in bin_dir
        self.logger.info(
            "writing filtered binned contigs summary file "
            + run_config['summary_file_path'] + '-new'
        )

        with open(run_config['summary_file_path'] + '-new', 'w', newline='') as summary_fh:
            summary_writer = self._init_summary_writer(summary_fh)

            # with open(run_config['bin_stats_ext_file'], 'r') as bin_stats_fh:
            #     for bin_stats_line in bin_stats_fh:
            #         bin_stats_line.rstrip()
            #         [full_bin_ID, bin_stats_json_str] = bin_stats_line.split("\t")

            for bid in sorted(bin_stats_raw_data.keys()):

                # bin_id = self.checkMUtil.clean_bin_ID(bid)
                # bin_stats[bid]['Bin Name'] = bin_id

                # full_bin_ID is in the form bin.xxx, so strip off the 'bin' prefix
                bin_ID = self.checkMUtil.clean_bin_ID(bid, run_config['fasta_ext'])
                bin_stats_raw_data[bid]['Bin Name'] = bin_ID

                # convert the raw data to JSON and reparse
                bin_stats_data[bin_ID] = json.loads(
                    json.dumps(bin_stats_raw_data[bid]),
                    parse_float=Decimal
                )

                comp = float(bin_stats_data[bin_ID]['Completeness'])
                cont = float(bin_stats_data[bin_ID]['Contamination'])
                bin_is_HQ = True

                if test_completeness and comp < completeness_thresh:
                    bin_is_HQ = False
                    self.log_completeness_fail(bin_ID, comp, completeness_thresh)

                if test_contamination and cont > contamination_thresh:
                    bin_is_HQ = False
                    self.log_contamination_fail(bin_ID, cont, contamination_thresh)

                bin_stats_data[bin_ID]['QA Pass'] = bin_is_HQ

                self.logger.debug({'event': 'bin_stats_read', bin_ID: bin_stats_data[bin_ID]})

                if not bin_is_HQ:
                    self.logger.info(
                        "Bin " + bin_ID + " didn't pass QC filters. Skipping."
                    )
                    removed_bin_IDs[bin_ID] = True
                else:
                    self.logger.info(
                        "Bin " + bin_ID + " passed QC filters. Adding to new BinnedContigs"
                    )
                    retained_bin_IDs[bin_ID] = True

                    # copy filtered bin scaffold files to filtered dir
                    src_path = bin_fasta_files_by_bin_ID[bin_ID]
                    dst_path = os.path.join(
                        filtered_bins_dir,
                        bin_basename + '.' + str(bin_ID) + '.' + fasta_ext_bc
                    )

                    self.outputbuilder._copy_file_new_name_ignore_errors(src_path, dst_path)

                    if bin_ID in bin_summary_info:
                        summary_writer.writerow([
                            bin_summary_info[bin_ID]['name'],
                            bin_summary_info[bin_ID]['cov'],
                            bin_summary_info[bin_ID]['sum_contig_len'],
                            bin_summary_info[bin_ID]['gc'],
                        ])
                    else:
                        # wtf?
                        self.logger.error('No bin summary data found for ' + bin_ID)

        missing_ids = [bin_ID for bin_ID in bin_IDs if bin_ID not in bin_stats_data]
        if missing_ids:
            raise ValueError(
                "The following Bin IDs are missing from the checkM output: "
                + ", ".join(sorted(missing_ids))
            )

        setattr(self, 'bin_stats_data', bin_stats_data)
        # self.bin_stats_data = bin_stats_data

        self.logger.debug({
            'retained_bin_IDs': retained_bin_IDs,
            'removed_bin_IDs': removed_bin_IDs,
        })

        if not retained_bin_IDs or not removed_bin_IDs:
            if not removed_bin_IDs:
                self.logger.warning('No bins removed by filtering: no new BinnedContigs created.')
            # delete the filtered bins dir
            shutil.rmtree(filtered_bins_dir, ignore_errors=True)
            return None

        # create BinnedContig object from filtered bins
        # self.build_bin_summary_file_from_binnedcontigs_obj(params, binned_contig_obj, retained_bin_IDs)
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
            "Bin " + bin_ID + ": contamination of " + str(value)
            + " above thresh " + str(threshold)
        )

    def log_completeness_fail(self, bin_ID, value, threshold):
        self.logger.info(
            "Bin " + bin_ID + ": completeness of " + str(value)
            + " below thresh " + str(threshold)
        )

    def _init_summary_writer(self, summary_fh):

        summary_writer = self.init_tsv_writer(summary_fh)
        summary_writer.writerow(['Bin name', 'Completeness', 'Genome size', 'GC content'])

        return summary_writer

    def extract_binned_contigs_data(self, binned_contig_obj):

        run_config = self.run_config()
        bin_summary_info = {}
        bin_basename = run_config['bin_basename']
        fasta_ext = run_config['fasta_ext']

        self.logger.debug({'binned_contig_obj_keys': binned_contig_obj.keys()})

        for bin_item in binned_contig_obj['bins']:
            bin_ID = self.checkMUtil.clean_bin_ID(bin_item['bid'], fasta_ext)
            bin_summary_info[bin_ID] = {
                'name': ".".join([bin_basename, str(bin_ID), fasta_ext]),
                'cov':  str(round(100.0 * float(bin_item['cov']), 1)) + '%',
                'sum_contig_len': str(bin_item['sum_contig_len']),
                'gc':   str(round(100.0 * float(bin_item['gc']), 1)),
            }

            self.logger.debug({
                "bin_item[bid]": bin_item['bid'],
                'clean_id': bin_ID,
                'summary_info': bin_summary_info[bin_ID],
            })  # DEBUG

        return bin_summary_info

    def build_bin_summary_file_from_binnedcontigs_obj(self, params, binned_contig_obj, retained_bin_IDs):

        run_config = self.run_config()
        fasta_ext = run_config['fasta_ext']
        bin_basename = run_config['bin_basename']

        dsu = self.datastagingutils
        filtered_bin_ID_dict = dsu.get_bin_fasta_files(run_config['filtered_bins_dir'], fasta_ext)
        filtered_bin_IDs = []

        for bin_ID in sorted(filtered_bin_ID_dict.keys()):
            self.logger.debug({
                'original_key': bin_ID,
                'cleaned_key': self.checkMUtil.clean_bin_ID(bin_ID, fasta_ext),
            })
            filtered_bin_IDs.append(self.checkMUtil.clean_bin_ID(bin_ID, fasta_ext))

        self.logger.debug({'filtered_bin_IDs': filtered_bin_IDs})
        self.logger.debug({'retained_bin_IDs': retained_bin_IDs})

        bin_summary_info = self.extract_binned_contigs_data(binned_contig_obj)
        # bin_summary_info = dict()

        # # fetch the existing binned_contig object
        # binned_contig_obj = self.workspacehelper.get_obj_from_workspace(params['input_ref'])

        # # bid in object is full name of contig fasta file. want just the number
        # for bin_item in binned_contig_obj['bins']:
        #     bin_ID = self.checkMUtil.clean_bin_ID(bin_item['bid'], fasta_ext)

        #     bin_summary_info[bin_ID] = {
        #         'n_contigs':        bin_item['n_contigs'],
        #         'gc':               round(100.0 * float(bin_item['gc']), 1),
        #         'sum_contig_len':   bin_item['sum_contig_len'],
        #         'cov':              round(100.0 * float(bin_item['cov']), 1),
        #     }
        # self.logger.debug({'bin_summary_info': bin_summary_info})
        # write summary file for just those bins present in bin_dir
        summary_file_path = run_config['summary_file_path']
        self.logger.info("writing filtered binned contigs summary file " + summary_file_path)

        with open(summary_file_path, 'w') as summary_file_handle:

            header_line = ['Bin name', 'Completeness', 'Genome size', 'GC content']
            summary_file_handle.write("\t".join(header_line)+"\n")

            for bin_ID in filtered_bin_IDs:
                # self.logger.debug("EXAMINING BIN SUMMARY INFO FOR BIN_ID: "+bin_ID)  # DEBUG
                bin_summary_info_line = [
                    bin_basename + '.' + str(bin_ID) + '.' + fasta_ext,
                    str(bin_summary_info[bin_ID]['cov'])+'%',
                    str(bin_summary_info[bin_ID]['sum_contig_len']),
                    str(bin_summary_info[bin_ID]['gc'])
                ]
                summary_file_handle.write("\t".join(bin_summary_info_line)+"\n")
                self.logger.debug({'bin_summary_line': bin_summary_info_line})

        return summary_file_path

    def save_binned_contigs(self, params, assembly_ref):

        run_config = self.run_config()
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
