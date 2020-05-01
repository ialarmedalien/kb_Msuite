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

        This method creates a directory in the scratch area with the set of Fasta files, names
        will have the fasta_ext run_config (run_config['fasta_ext']) parameter tacked on.

            ex:

            staged_input = stage_input('124/15/1', 'fna')

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

    def process_assembly_contigset(self, input_ref, input_dir, fasta_ext, obj_name, obj_type):

        auClient = self.client('AssemblyUtil')

        # create file data
        filename = os.path.join(input_dir, obj_name + '.' + fasta_ext)
        auClient.get_assembly_as_fasta({'ref': input_ref, 'filename': filename})
        if not os.path.isfile(filename):
            raise ValueError('Error generating fasta file from an Assembly or ContigSet with AssemblyUtil')
        # make sure fasta file isn't empty
        min_fasta_len = 1
        if not self.fasta_seq_len_at_least(filename, min_fasta_len):
            raise ValueError('Assembly or ContigSet is empty in filename: ' + str(filename))

        self.logger.debug('Saved assembly or contigset to ' + filename)

        return True

    def process_assembly_set(self, input_ref, input_dir, fasta_ext, obj_name, obj_type):

        setAPI_Client = self.client('SetAPI')
        auClient = self.client('AssemblyUtil')

        # read assemblySet
        try:
            assemblySet_obj = setAPI_Client.get_assembly_set_v1({
                    'ref': input_ref,
                    'include_item_info': 1
                })
        except Exception as e:
            raise ValueError('Unable to get object from workspace: (' + input_ref + ')' + str(e))

#         assembly_refs = []
#         assembly_names = []
        for assembly_item in assemblySet_obj['data']['items']:

            assembly_ref = assembly_item['ref']
            assembly_info = None
            # assembly obj info
            try:
                assembly_info = self.client('Workspace').get_object_info_new({
                    'objects': [{'ref': assembly_ref}]
                })[0]
            except Exception as e:
                raise ValueError('Unable to get object from workspace: (' + assembly_ref + '): ' + str(e))
            self.logger.debug({'assembly info': assembly_info})
            assembly_name = assembly_info[1]
            filename = os.path.join(input_dir, assembly_name + '.' + fasta_ext)
            auClient.get_assembly_as_fasta({'ref': assembly_ref, 'filename': filename})

            if not os.path.isfile(filename):
                raise ValueError('Error generating fasta file from an Assembly or ContigSet with AssemblyUtil')
            # make sure fasta file isn't empty
            min_fasta_len = 1
            if not self.fasta_seq_len_at_least(filename, min_fasta_len):
                raise ValueError('Assembly or ContigSet is empty in filename: ' + str(filename))

            self.logger.info('saved ' + assembly_name + ' to ' + filename)
#             assembly_refs.append(this_assembly_ref)
#             assembly_names.append(this_assembly_name)
#
#         self.logger.debug({'assembly_refs': assembly_refs, 'assembly_names': assembly_names})

#         create file data (name for file is what's reported in results)
#         for ass_i, assembly_ref in enumerate(assembly_refs):
#             this_name = assembly_names[ass_i]
#             filename = os.path.join(input_dir, this_name + '.' + fasta_ext)
#             auClient.get_assembly_as_fasta({'ref': assembly_ref, 'filename': filename})
#             self.logger.debug('ass ref: ' + assembly_ref + '; filename: ' + filename)
#
#             if not os.path.isfile(filename):
#                 raise ValueError('Error generating fasta file from an Assembly or ContigSet with AssemblyUtil')
#             make sure fasta file isn't empty
#             min_fasta_len = 1
#             if not self.fasta_seq_len_at_least(filename, min_fasta_len):
#                 raise ValueError('Assembly or ContigSet is empty in filename: ' + str(filename))

        return True

    def process_binned_contigs(self, input_ref, input_dir, fasta_ext, obj_name, obj_type):

        mguClient = self.client('MetagenomeUtils')

        # download the bins as fasta and set the input folder name
        file_result = mguClient.binned_contigs_to_file({
            'input_ref': input_ref,
            'save_to_shock': 0,
        })
        bin_file_dir = file_result['bin_file_directory']
        self.logger.info('Renaming ' + bin_file_dir + ' to ' + input_dir)
        os.rename(bin_file_dir, input_dir)
        # make sure fasta file isn't empty
        self.set_fasta_file_extensions(input_dir, fasta_ext)
        for (dirpath, dirnames, filenames) in os.walk(input_dir):
            self.logger.debug({
                'dirpath': dirpath,
                'dirnames': dirnames,
                'filenames': filenames,
            })

            for fasta_file in filenames:
                fasta_path = os.path.join(input_dir, fasta_file)
                min_fasta_len = 1
                if not self.fasta_seq_len_at_least(fasta_path, min_fasta_len):
                    raise ValueError('Binned Assembly is empty for fasta_path: ' + str(fasta_path))
                self.logger.info('Processed valid binned contig file ' + fasta_path)
            break

        return True

    def process_genome_genome_set(self, input_ref, input_dir, fasta_ext, obj_name, obj_type):

        auClient = self.client('AssemblyUtil')

        genome_obj_names = []
        genome_sci_names = []
        genome_assembly_refs = []

        if obj_type == 'KBaseGenomes.Genome':
            genomeSet_refs = [input_ref]
        else:  # get genomeSet_refs from GenomeSet object
            genomeSet_refs = []
            genomeSet_object = self.workspacehelper.get_obj_from_workspace(input_ref)

            self.logger.debug({'genomeset_object': genomeSet_object})
            # iterate through genomeSet members
            for genome_id in list(genomeSet_object['elements'].keys()):
                if 'ref' not in genomeSet_object['elements'][genome_id] or \
                  genomeSet_object['elements'][genome_id]['ref'] is None or \
                  genomeSet_object['elements'][genome_id]['ref'] == '':
                    raise ValueError('genome_ref not found for genome_id: ' + str(genome_id) + ' in genomeSet: ' + str(input_ref))
                else:
                    genomeSet_refs.append(genomeSet_object['elements'][genome_id]['ref'])

        # genome obj data
        for i, this_input_ref in enumerate(genomeSet_refs):

            objects = self.client('Workspace').get_objects2({'objects': [{'ref': this_input_ref}]})['data']
            genome_obj = objects[0]['data']
            genome_obj_info = objects[0]['info']
            genome_obj_names.append(genome_obj_info[1])
            genome_sci_names.append(genome_obj['scientific_name'])

            # Get genome_assembly_ref
            if ('contigset_ref' not in genome_obj or genome_obj['contigset_ref'] is None) \
               and ('assembly_ref' not in genome_obj or genome_obj['assembly_ref'] is None):
                msg = "Genome "+genome_obj_names[i]+" (ref:"+input_ref+") "+genome_sci_names[i]+" MISSING BOTH contigset_ref AND assembly_ref. Cannot process. Exiting."
                raise ValueError(msg)
                continue
            elif 'assembly_ref' in genome_obj and genome_obj['assembly_ref'] is not None:
                msg = "Genome "+genome_obj_names[i]+" (ref:"+input_ref+") "+genome_sci_names[i]+" USING assembly_ref: "+str(genome_obj['assembly_ref'])
                self.logger.info(msg)
                genome_assembly_refs.append(genome_obj['assembly_ref'])
            elif 'contigset_ref' in genome_obj and genome_obj['contigset_ref'] is not None:
                msg = "Genome "+genome_obj_names[i]+" (ref:"+input_ref+") "+genome_sci_names[i]+" USING contigset_ref: "+str(genome_obj['contigset_ref'])
                self.logger.info(msg)
                genome_assembly_refs.append(genome_obj['contigset_ref'])

        # create file data (name for file is what's reported in results)
        for ass_i, assembly_ref in enumerate(genome_assembly_refs):
            this_name = genome_obj_names[ass_i]
            filename = os.path.join(input_dir, this_name + '.' + fasta_ext)
            auClient.get_assembly_as_fasta({'ref': assembly_ref, 'filename': filename})
            if not os.path.isfile(filename):
                raise ValueError('Error generating fasta file from an Assembly or ContigSet with AssemblyUtil')
            # make sure fasta file isn't empty
            min_fasta_len = 1
            if not self.fasta_seq_len_at_least(filename, min_fasta_len):
                raise ValueError('Assembly or ContigSet is empty in filename: '+str(filename))

        return True

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
        fasta_file_handle = open(output_fasta_file, 'w')
        p = subprocess.Popen(cat_cmd, cwd=self.scratch, stdout=fasta_file_handle, shell=False)
        exitCode = p.wait()
        fasta_file_handle.close()

        if exitCode != 0:
            raise ValueError('Error running command: ' + ' '.join(cat_cmd) + '\n' +
                             'Exit Code: ' + str(exitCode))

    def get_bin_fasta_files(self, search_dir, fasta_ext):

        bin_fasta_files = dict()
        for (dirpath, dirnames, filenames) in os.walk(search_dir):
            # DEBUG
            # self.logger.debug({'dirpath': dirpath, 'dirnames': dirnames, 'filenames': filenames})
            for filename in filenames:
                if not os.path.isfile(os.path.join(search_dir, filename)):
                    continue
                if filename.endswith('.' + fasta_ext):
                    bin_ID = self.checkMUtil.clean_bin_ID(filename, fasta_ext)
                    bin_fasta_files[bin_ID] = os.path.join(search_dir, filename)
                    # self.logger.debug("ACCEPTED: "+bin_ID+" FILE:"+filename)  # DEBUG

        return bin_fasta_files
