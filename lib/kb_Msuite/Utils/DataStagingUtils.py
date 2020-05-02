import os
import glob
import subprocess

from kb_Msuite.Utils.Utils import Base, LogMixin


class DataStagingUtils(Base, LogMixin):

    def __init__(self, checkMUtil_obj):
        self.checkMUtil = checkMUtil_obj
        self.client_util = checkMUtil_obj.client_util
        config = checkMUtil_obj.config
        self.scratch = os.path.abspath(config['scratch'])
        self.workspacehelper = self.checkMUtil.workspacehelper

        if not os.path.exists(self.scratch):
            os.makedirs(self.scratch)

    def client(self, client_name):

        return self.client_util.client(client_name)

    def run_config(self):

        return self.checkMUtil.run_config()

    def stage_input(self, input_ref):
        '''
        Stage input based on an input data reference for CheckM

        This method creates an input directory in the scratch area with the set of FASTA files
        File names will have the fasta extension (from run_config['fasta_ext']) tacked on.

            ex:

            staged_input = stage_input('124/15/1')

            staged_input
            {"input_dir": '...'}
        '''

        run_config = self.run_config()

        # 1) generate a folder in scratch to hold the input
        input_dir = run_config['input_dir']
        all_seq_fasta = run_config['all_seq_fasta']
        fasta_ext = run_config['fasta_ext']

        if not os.path.exists(input_dir):
            os.makedirs(input_dir)

        obj_info = self.workspacehelper.get_ws_obj_info(input_ref)
        obj_name = self.workspacehelper.get_ws_obj_name(object_info=obj_info)
        obj_type = self.workspacehelper.get_ws_obj_type(object_info=obj_info)

        type_to_method = {

            # Standard Single Assembly
            'KBaseGenomeAnnotations.Assembly': self.process_assembly_contigset,
            'KBaseGenomes.ContigSet': self.process_assembly_contigset,
            # AssemblySet
            'KBaseSets.AssemblySet': self.process_assembly_set,
            # Binned Contigs
            'KBaseMetagenomes.BinnedContigs': self.process_binned_contigs,
            # Genome and GenomeSet
            'KBaseGenomes.Genome': self.process_genome_genome_set,
            'KBaseSearch.GenomeSet': self.process_genome_genome_set,
        }

        if obj_type not in type_to_method:
            raise ValueError('Cannot stage fasta file input directory from type: ' + obj_type)

        type_to_method[obj_type](input_ref, input_dir, fasta_ext, obj_name, obj_type)

        # create summary fasta file with all bins
        self.cat_fasta_files(input_dir, fasta_ext, all_seq_fasta)

        return {
            'obj_name': obj_name,
            'obj_type': obj_type,
        }

    def process_assembly_contigset(self, input_ref, input_dir, fasta_ext, obj_name, obj_type=None):
        '''
        Given the input parameters, saves the assembly or contigset in the designated
        input directory with the name <object_name>.<fasta_ext>
        '''

        # create file data
        filename = os.path.join(input_dir, obj_name + '.' + fasta_ext)
        self.client('AssemblyUtil').get_assembly_as_fasta({'ref': input_ref, 'filename': filename})

        if not os.path.isfile(filename):
            raise ValueError(
                'Error generating fasta file from an Assembly or ContigSet with AssemblyUtil'
            )
        # make sure fasta file isn't empty
        min_fasta_len = 1
        if not self.fasta_seq_len_at_least(filename, min_fasta_len):
            raise ValueError('Assembly or ContigSet is empty in filename: ' + str(filename))

        self.logger.debug('Saved assembly or contigset to ' + filename)

        return True

    def process_assembly_set(self, input_ref, input_dir, fasta_ext, obj_name, obj_type):
        '''
        Given an assemblyset, retrieve the sequence of each member and save it to input_dir

        '''

        # read assemblySet
        try:
            assembly_set_obj = self.client('SetAPI').get_assembly_set_v1({
                    'ref': input_ref,
                    'include_item_info': 1,
                })
        except Exception as e:
            raise ValueError('Unable to get object from workspace: (' + input_ref + ')' + str(e))

        self.logger.debug({'assembly_set_obj': assembly_set_obj})

        # 'assembly_set_obj' = {
        #     'data': {
        #         'description': 'test assembly set',
        #         'items': [{
        #             'label': 'assembly_1',
        #             'ref': '49697/1/1',
        #             'info': [1, 'Test.Assembly', 'KBaseGenomeAnnotations.Assembly-6.0', '2020-04-29T18:03:17+0000', 1, 'ialarmedalien', 49697, 'test_kb_Msuite_refdata_1588183380977', '656b6409ed3b7ffdfa00247f9834c717', 208652, {'GC content': '0.45594', 'Size': '8397583', 'N Contigs': '922', 'MD5': 'bc1005f1fc28e132389f017ba9c42897'}]
        #         },
        #         {
        #             'label': 'assembly_2',
        #             'ref': '49697/2/1',
        #             'info': [2, 'Dodgy_Contig.Assembly', 'KBaseGenomeAnnotations.Assembly-6.0', '2020-04-29T18:03:42+0000', 1, 'ialarmedalien', 49697, 'test_kb_Msuite_refdata_1588183380977', '5294dba9811f03769dac2b8104cfbc45', 752, {'GC content': '0.63818', 'Size': '7360', 'N Contigs': '1', 'MD5': 'c586bdf420a97d2a5ea75dac3b0f25cc'}]
        #         }]
        #     },
        #     'info': [3, 'TEST_ASSEMBLY_SET', 'KBaseSets.AssemblySet-2.1', '2020-04-29T18:03:45+0000', 1, 'ialarmedalien', 49697, 'test_kb_Msuite_refdata_1588183380977', 'd1a2540c6db724e77b32885cfb26fab2', 127, {'item_count': '2', 'description': 'test assembly set'}]
        # }

        for assembly_item in assembly_set_obj['data']['items']:
            assembly_ref = assembly_item['ref']
            assembly_info = assembly_item['info']
            self.logger.debug({'assembly info': assembly_info})
            assembly_name = self.workspacehelper.get_ws_obj_name(object_info=assembly_info)

            # process this with the standard assembly processing
            self.process_assembly_contigset(assembly_ref, input_dir, fasta_ext, assembly_name)

        return True

    def process_binned_contigs(self, input_ref, input_dir, fasta_ext, obj_name, obj_type):

        # download the bins as fasta and set the input folder name
        file_result = self.client('MetagenomeUtils').binned_contigs_to_file({
            'input_ref': input_ref,
            'save_to_shock': 0,
        })
        bin_file_dir = file_result['bin_file_directory']
        self.logger.debug('Renaming ' + bin_file_dir + ' to ' + input_dir)
        os.rename(bin_file_dir, input_dir)
        self.set_fasta_file_extensions(input_dir, fasta_ext)
        for (dirpath, dirnames, filenames) in os.walk(input_dir):
            for fasta_file in filenames:
                fasta_path = os.path.join(input_dir, fasta_file)
                # make sure fasta file isn't empty
                min_fasta_len = 1
                if not self.fasta_seq_len_at_least(fasta_path, min_fasta_len):
                    raise ValueError('Binned Assembly is empty for fasta_path: ' + str(fasta_path))
                self.logger.info('Processed valid binned contig file ' + fasta_path)
            break

        return True

    def process_genome_genome_set(self, input_ref, input_dir, fasta_ext, obj_name, obj_type):

        genome_set_refs = []

        if obj_type == 'KBaseGenomes.Genome':
            genome_set_refs = [input_ref]
        else:  # get genomeSet_refs from GenomeSet object
            genome_set_refs = []
            genomeSet_object = self.workspacehelper.get_obj_from_workspace(input_ref)
            # self.logger.debug({'genomeset_object': genomeSet_object})

            # iterate through genomeSet members
            for genome_id in list(genomeSet_object['elements'].keys()):
                if 'ref' not in genomeSet_object['elements'][genome_id] or \
                  genomeSet_object['elements'][genome_id]['ref'] is None or \
                  genomeSet_object['elements'][genome_id]['ref'] == '':
                    raise ValueError(
                        'genome_ref not found for genome_id: ' + str(genome_id)
                        + ' in genomeSet: ' + str(input_ref)
                    )

                genome_set_refs.append(genomeSet_object['elements'][genome_id]['ref'])

        # genome obj data
        for genome_ref in genome_set_refs:

            objects = self.client('Workspace').get_objects2({
                'objects': [{'ref': genome_ref}]
            })['data']
            self.logger.debug({'genome_object': objects})
            genome_obj = objects[0]['data']
            genome_obj_info = objects[0]['info']
            genome_name = self.workspacehelper.get_ws_obj_name(object_info=genome_obj_info)
            genome_sci_name = genome_obj['scientific_name']

            genome_str = self.genome_data_format(genome_name, genome_sci_name, input_ref)

            # Get genome assembly_ref
            if ('contigset_ref' not in genome_obj or genome_obj['contigset_ref'] is None) \
              and ('assembly_ref' not in genome_obj or genome_obj['assembly_ref'] is None):
                msg = genome_str + " MISSING BOTH contigset_ref AND assembly_ref. " \
                    + "Cannot process. Exiting."
                raise ValueError(msg)

            if 'assembly_ref' in genome_obj and genome_obj['assembly_ref'] is not None:
                obj_ref = genome_obj['assembly_ref']
                msg = genome_str + " USING assembly_ref: "
            elif 'contigset_ref' in genome_obj and genome_obj['contigset_ref'] is not None:
                obj_ref = genome_obj['contigset_ref']
                msg = genome_str + " USING contigset_ref: "

            self.logger.info(msg + str(obj_ref))
            # this can be processed as an assembly now
            self.process_assembly_contigset(obj_ref, input_dir, fasta_ext, genome_name)

        return True

    def genome_data_format(self, genome_name, genome_sci_name, input_ref):

        return 'Genome ' + genome_name + " (ref:" + input_ref + ") " + genome_sci_name

    def fasta_seq_len_at_least(self, fasta_path, min_fasta_len=1):
        '''
        counts the number of non-header, non-whitespace characters in a FASTA file
        '''
        seq_len = 0
        with open(fasta_path, 'r') as fasta_handle:
            for line in fasta_handle:
                line = line.strip()
                if line.startswith('>'):
                    continue
                line = line.replace(' ', '')
                seq_len += len(line)
                if seq_len >= min_fasta_len:
                    return True
        return False

    def set_fasta_file_extensions(self, folder, new_extension):
        '''
        Renames all detected fasta files in folder to the specified extension.
        fasta files are detected based on its existing extension, which must be one of:
            ['.fasta', '.fas', '.fa', '.fsa', '.seq', '.fna', '.ffn', '.faa', '.frn']

        Note that this is probably not well behaved if the operation will rename to a
        file that already exists
        '''
        extensions = ['.fasta', '.fas', '.fa', '.fsa', '.seq', '.fna', '.ffn', '.faa', '.frn']

        for file in os.listdir(folder):
            if not os.path.isfile(os.path.join(folder, file)):
                continue
            filename, file_extension = os.path.splitext(file)
            if file_extension in extensions:
                os.rename(os.path.join(folder, file),
                          os.path.join(folder, filename + '.' + new_extension))

    def cat_fasta_files(self, folder, extension, output_fasta_file):
        '''
        Given a folder of fasta files with the specified extension, cat them together
        using 'cat' into the target new_fasta_file
        '''
        files = glob.glob(os.path.join(folder, '*.' + extension))
        cat_cmd = ['cat'] + files
        fasta_fh = open(output_fasta_file, 'w')
        p = subprocess.Popen(cat_cmd, cwd=self.scratch, stdout=fasta_fh, shell=False)
        exitCode = p.wait()
        fasta_fh.close()

        if exitCode != 0:
            raise ValueError('Error running command: ' + ' '.join(cat_cmd) + '\n' +
                             'Exit Code: ' + str(exitCode))

    def get_bin_fasta_files(self, search_dir, fasta_ext):

        bin_fasta_files = dict()
        for (dirpath, dirnames, filenames) in os.walk(search_dir):
            for filename in filenames:
                if not os.path.isfile(os.path.join(search_dir, filename)):
                    continue
                if filename.endswith('.' + fasta_ext):
                    bin_ID = self.checkMUtil.clean_bin_ID(filename, fasta_ext)
                    bin_fasta_files[bin_ID] = os.path.join(search_dir, filename)
                    # self.logger.debug("ACCEPTED: "+bin_ID+" FILE:"+filename)  # DEBUG

        return bin_fasta_files
