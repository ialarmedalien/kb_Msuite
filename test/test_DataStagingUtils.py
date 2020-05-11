import os
from CheckMTestBase import CoreCheckMTestClient


class TestDataStagingUtils(CoreCheckMTestClient):

    def test_02_data_staging(self):

        # Standard Single Assembly
        # 'KBaseGenomeAnnotations.Assembly': self.process_assembly_contigset,
        # 'KBaseGenomes.ContigSet': self.process_assembly_contigset, -- TODO
        # AssemblySet
        # 'KBaseSets.AssemblySet': self.process_assembly_set,
        # Binned Contigs
        # 'KBaseMetagenomes.BinnedContigs': self.process_binned_contigs,
        # Genome and GenomeSet
        # 'KBaseGenomes.Genome': self.process_genome_genome_set,
        # 'KBaseSearch.GenomeSet': self.process_genome_genome_set,
        # also test:
        # - empty versions of each of these

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging")
        self.logger.info("=================================================================\n")

        self.require_data('report_ref', 'binned_contigs_empty_ref')

        cmu = self.checkMUtil

        with self.subTest('erroneous report object staging'):
            err_msg = 'Cannot stage fasta file input directory from type: KBaseReport.Report'
            with self.assertRaisesRegex(ValueError, err_msg):
                cmu.datastagingutils.stage_input(self.report_ref)

        # empty input files/objects:
        # cannot create an empty assembly or an empty genome object
        # can create a binnedcontigs obj with an empty assembly
        with self.subTest('One empty assembly in BinnedContigs'):
            err_str = 'Binned Assembly is empty for fasta_path: '
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.datastagingutils.stage_input(self.binned_contigs_empty_ref)

        with self.subTest('invalid workspace ref'):
            err_msg = 'Unable to perform Workspace command "get_object_info3": '
            with self.assertRaisesRegex(ValueError, err_msg):
                cmu.datastagingutils.stage_input('here_is_a_made_up_ref')

    def check_data_staging_results(self, run_config, filenames):

        self.assertTrue(os.path.isdir(run_config['input_dir']))
        self.assertTrue(os.path.isfile(run_config['all_seq_fasta']))
        for name in filenames:
            self.assertTrue(os.path.isfile(os.path.join(
                run_config['input_dir'], name + '.' + run_config['fasta_ext'])
            ))

    def test_02_data_staging_assembly(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_assembly")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_mini_ref')
        cmu = self.checkMUtil

        staged_input = cmu.datastagingutils.stage_input(self.assembly_mini_ref)
        self.assertEqual(
            staged_input,
            {'obj_name': 'MiniAssembly', 'obj_type': 'KBaseGenomeAnnotations.Assembly'}
        )
        self.check_data_staging_results(cmu.run_config(), ['MiniAssembly'])

    def test_02_data_staging_assembly_strange_fasta_ext(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_assembly_strange_fasta_ext")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_OK_ref')

        cmu = self.checkMUtil
        cmu.fasta_extension = 'strange_fasta_extension'
        run_config = cmu._set_run_config()
        staged_input = cmu.datastagingutils.stage_input(self.assembly_OK_ref)
        self.assertEqual(
            staged_input,
            {'obj_name': 'Test.Assembly', 'obj_type': 'KBaseGenomeAnnotations.Assembly'}
        )
        self.check_data_staging_results(run_config, ['Test.Assembly'])

    def test_02_data_staging_assemblyset(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_assemblyset")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_set_ref')
        cmu = self.checkMUtil
        staged_input = cmu.datastagingutils.stage_input(self.assembly_set_ref)
        self.assertEqual(
            staged_input,
            {
                'obj_name': 'TEST_ASSEMBLY_SET',
                'obj_type': 'KBaseSets.AssemblySet',
            }
        )
        self.check_data_staging_results(
            cmu.run_config(), ['Test.Assembly', 'Dodgy_Contig.Assembly']
        )

    def test_02_data_staging_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref')

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        # test stage binned contigs
        staged_input = cmu.datastagingutils.stage_input(self.binned_contigs_ref)
        # expect to get back {'obj_name': name, 'obj_type': type}
        self.assertEqual(staged_input, {
            'obj_name': 'Binned_Contigs',
            'obj_type': 'KBaseMetagenomes.BinnedContigs'
        })
        self.assertTrue(os.path.isdir(run_config['input_dir']))
        self.assertTrue(os.path.isfile(run_config['all_seq_fasta']))

        # three binned contigs
        for number in ["1", "2", "3"]:
            self.assertTrue(os.path.isfile(os.path.join(
                run_config['input_dir'], 'bin.00' + number + '.' + run_config['fasta_ext']
            )))

    def test_02_data_staging_genome(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_genome")
        self.logger.info("=================================================================\n")

        self.require_data('genome_refs')

        cmu = self.checkMUtil
        test_genome = self.get_data()['genome_list'][0]

        staged_input = cmu.datastagingutils.stage_input(getattr(self, test_genome['attr']))
        self.assertEqual(
            staged_input,
            {
                'obj_name': test_genome['name'],
                'obj_type': 'KBaseGenomes.Genome',
            }
        )
        self.check_data_staging_results(cmu.run_config(), [test_genome['name']])

    def test_02_data_staging_genome_set(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_genome_set")
        self.logger.info("=================================================================\n")

        self.require_data('genome_set_small_ref')

        cmu = self.checkMUtil
        run_config = cmu.run_config()
        genome_list = self.get_data()['genome_list'][0:3]

        staged_input = cmu.datastagingutils.stage_input(self.genome_set_small_ref)
        self.assertEqual(
            staged_input,
            {
                'obj_name': 'Small_GenomeSet',
                'obj_type': 'KBaseSearch.GenomeSet',
            }
        )
        self.check_data_staging_results(run_config, [g['name'] for g in genome_list])
