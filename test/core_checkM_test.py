# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import shutil

from os import environ
from configparser import ConfigParser
from pprint import pprint  # noqa: F401

from installed_clients.WorkspaceClient import Workspace
from installed_clients.AssemblyUtilClient import AssemblyUtil
from installed_clients.SetAPIServiceClient import SetAPI
from installed_clients.GenomeFileUtilClient import GenomeFileUtil
from installed_clients.MetagenomeUtilsClient import MetagenomeUtils

from kb_Msuite.kb_MsuiteImpl import kb_Msuite
from kb_Msuite.kb_MsuiteServer import MethodContext
from kb_Msuite.authclient import KBaseAuth as _KBaseAuth

from kb_Msuite.Utils.CheckMUtil import CheckMUtil
from kb_Msuite.Utils.DataStagingUtils import DataStagingUtils
from kb_Msuite.Utils.OutputBuilder import OutputBuilder
from kb_Msuite.Utils.ClientUtil import ClientUtil


def print_method_name(method):
    def wrapper(*args, **kwargs):
        method_name = method.__name__
        method_name.replace("test_", "")
        print("\n=================================================================")
        print(("RUNNING " + method_name + "()"))
        print("=================================================================\n")
        return method(*args, **kwargs)
    return wrapper

class CoreCheckMTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        init_time_stamp = time.time()
        token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        test_time_stamp = int(time.time() * 1000)
        environ['KB_TEST_ID'] = str(test_time_stamp)

        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('kb_Msuite'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({
            'token': token,
            'user_id': user_id,
            'provenance': [{
                'service': 'kb_Msuite',
                'method': 'please_never_use_it_in_production',
                'method_params': []
            }],
            'authenticated': 1
        })
        cls.wsURL       = cls.cfg['workspace-url']
        cls.wsClient    = Workspace(cls.wsURL)
        cls.serviceImpl = kb_Msuite(cls.cfg)
        cls.callback_url = os.environ['SDK_CALLBACK_URL']
        cls.scratch     = cls.cfg['scratch']
        cls.suffix      = test_time_stamp
        cls.checkm_runner = CheckMUtil(cls.cfg, cls.ctx)

        cls.wsName  = "test_kb_Msuite_" + str(cls.suffix)
        cls.ws_info = cls.wsClient.create_workspace({'workspace': cls.wsName})
        cls.au      = AssemblyUtil(os.environ['SDK_CALLBACK_URL'])
        cls.gfu     = GenomeFileUtil(os.environ['SDK_CALLBACK_URL'], service_ver='dev')
        cls.mu      = MetagenomeUtils(os.environ['SDK_CALLBACK_URL'])
        cls.setAPI  = SetAPI(url=cls.cfg['srv-wiz-url'], token=cls.ctx['token'])

        # stage an input and output directory
        """
        cls.input_dir = os.path.join(cls.scratch, 'input_1')
        cls.output_dir = os.path.join(cls.scratch, 'output_1')
        cls.all_seq_fasta = os.path.join(cls.scratch, 'all_seq.fna')
        shutil.copytree(os.path.join('data', 'example_out', 'input'), cls.input_dir)
        shutil.copytree(os.path.join('data', 'example_out', 'output'), cls.output_dir)
        shutil.copy(os.path.join('data', 'example_out', 'all_seq.fna'), cls.all_seq_fasta)
        """

        # prepare WS data
        cls.prepare_data()
        end_time_stamp = time.time()
        print("set up time: " + str(end_time_stamp - init_time_stamp))

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print(('Test workspace ' + cls.wsName + ' was deleted'))
        pass

    def getWsClient(self):
        return self.__class__.wsClient

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def getConfig(self):
        return self.__class__.serviceImpl.config

    @classmethod
    def prepare_data(cls):
        test_directory_name = 'test_kb_Msuite'
        cls.test_directory_path = os.path.join(cls.scratch, test_directory_name)
        os.makedirs(cls.test_directory_path)

        test_data_dir = os.path.join(cls.scratch, 'test_data')
        os.makedirs(cls.test_directory_path)


        [OBJID_I, NAME_I, TYPE_I, SAVE_DATE_I, VERSION_I, SAVED_BY_I, WSID_I,
         WORKSPACE_I, CHSUM_I, SIZE_I, META_I] = list(range(11))  # object_info tuple

        assembly_list = [
            {
                # example assembly
                'path': 'assembly.fasta',
                'name': 'Test.Assembly',
                'attr': 'assembly_OK_ref',
            },
            {
                # contig that breaks checkm v1.0.7 reduced_tree (works on v1.0.8)
                'path': 'offending_contig_67815-67907.fa',
                'name': 'Dodgy_Contig.Assembly',
                'attr': 'assembly_dodgy_ref',
            }
        ]

        for assembly in assembly_list:
            assembly_file_path = os.path.join(test_data_dir, assembly['path'])
            shutil.copy(os.path.join("data", assembly['path']), assembly_file_path)
            saved_assembly = cls.au.save_assembly_from_fasta({
                'file': {'path': assembly_file_path},
                'workspace_name': cls.ws_info[1],
                'assembly_name': assembly['name'],
            })
            setattr(cls, assembly['attr'], saved_assembly)
            pprint('Saved Assembly: ' + getattr(cls, assembly['attr']))

        """
        # build the example Assembly
        assembly_filename = 'assembly.fasta'
        assembly_objname = 'Assembly_Test1.Assembly'
        assembly_fasta_file_path = os.path.join(cls.scratch, assembly_filename)
        shutil.copy(os.path.join("data", assembly_filename), assembly_fasta_file_path)
        assembly_params = {'file': {'path': assembly_fasta_file_path},
                           'workspace_name': cls.ws_info[1],
                           'assembly_name': assembly_objname
                           }
        cls.assembly_OK_ref = cls.au.save_assembly_from_fasta(assembly_params)
        pprint('Saved Assembly: ' + cls.assembly_OK_ref)

        # contig that breaks checkm v1.0.7 reduced_tree (works on v1.0.8)
        assembly_filename = 'offending_contig_67815-67907.fa'
        assembly_objname = 'Offending_Contig_test2.Assembly'
        assembly_fasta_file_path = os.path.join(cls.scratch, assembly_filename)
        shutil.copy(os.path.join("data", assembly_filename), assembly_fasta_file_path)
        assembly_params = {'file': {'path': assembly_fasta_file_path},
                           'workspace_name': cls.ws_info[1],
                           'assembly_name': assembly_objname
                           }
        cls.assembly_dodgy_ref = cls.au.save_assembly_from_fasta(assembly_params)
        pprint('Saved Assembly: ' + cls.assembly_dodgy_ref)
        """
        # create an AssemblySet
        assembly_items = [
            {'ref': cls.assembly_OK_ref, 'label': 'assembly_1'},
            {'ref': cls.assembly_dodgy_ref, 'label': 'assembly_2'}
        ]
        cls.assemblySet_ref = cls.setAPI.save_assembly_set_v1({
            'workspace_name': cls.ws_info[1],
            'output_object_name': 'TEST_ASSEMBLY_SET',
            'data': {
                'description': 'test assembly set',
                'items': assembly_items,
            },
        })['set_ref']

        binned_contigs_list = [
            {
                'path': 'binned_contigs',
                'name': 'Binned_Contigs',
            },
            {
                'path': 'binned_contigs_empty',
                'name': 'Binned_Contigs_Empty',
            }
        ]

        for bc in binned_contigs_list:
            binned_contigs_path = os.path.join(test_data_dir, bc['path'])
            shutil.copytree(os.path.join("data", bc['path']), binned_contigs_path)
            saved_object = cls.mu.file_to_binned_contigs({
                'file_directory': binned_contigs_path,
                'workspace_name': cls.ws_info[1],
                'assembly_ref': cls.assembly_OK_ref,
                'binned_contig_name': bc['name'],
            })
            setattr(cls, bc['path'] + '_ref', saved_object['binned_contig_obj_ref'])
            pprint('Saved BinnedContigs: ' + getattr(cls, bc['path'] + '_ref'))
        '''

        # create a BinnedContigs object
        binned_contigs_dir_name = 'binned_contigs'
        binned_contigs_objname = 'MyBins_test3'
        binned_contigs_dir_path = os.path.join(cls.scratch, binned_contigs_dir_name)
        shutil.copytree(os.path.join("data", binned_contigs_dir_name), binned_contigs_dir_path)

        binned_contigs_params = {'file_directory': binned_contigs_dir_path,
                                 'workspace_name': cls.ws_info[1],
                                 'assembly_ref': cls.assembly_OK_ref,
                                 'binned_contig_name': binned_contigs_objname
                                 }
        cls.binned_contigs_ref = cls.mu.file_to_binned_contigs(
                                                binned_contigs_params)['binned_contig_obj_ref']
        pprint('Saved BinnedContigs: ' + cls.binned_contigs_ref)

        # create an empty BinnedContigs object
        binned_contigs_dir_name_empty = 'binned_contigs_empty'
        binned_contigs_objname_empty = "MyBins_empty"
        binned_contigs_dir_path_empty = os.path.join(cls.scratch, binned_contigs_dir_name_empty)
        shutil.copytree(os.path.join("data", binned_contigs_dir_name_empty),
                        binned_contigs_dir_path_empty)

        binned_contigs_params = {'file_directory': binned_contigs_dir_path_empty,
                                 'workspace_name': cls.ws_info[1],
                                 'assembly_ref': cls.assembly_OK_ref,
                                 'binned_contig_name': binned_contigs_objname_empty
                                 }
        cls.binned_contigs_empty_ref = cls.mu.file_to_binned_contigs(
                                                binned_contigs_params)['binned_contig_obj_ref']
        pprint('Saved BinnedContigs: ' + cls.binned_contigs_empty_ref)

        '''

        # upload a few genomes
        cls.genome_refs = []
        for genome_filename in ['GCF_000022285.1_ASM2228v1_genomic.gbff',
                                'GCF_001439985.1_wTPRE_1.0_genomic.gbff']:
            genome_file_path = os.path.join(test_data_dir, genome_filename)
            shutil.copy(os.path.join("data", "genomes", genome_filename), genome_file_path)
            cls.genome_refs.append(cls.gfu.genbank_to_genome({
                    'file': {'path': genome_file_path},
                    'workspace_name': cls.ws_info[1],
                    'genome_name': genome_filename,
                    'generate_ids_if_needed': 1,
                })['genome_ref'])

        # create a genomeSet
        genome_scinames = dict()
        for genome_i, genome_ref in enumerate(cls.genome_refs):
            genome_scinames[genome_ref] = 'Genus species str. '+str(genome_i)
        testGS = {
            'description': 'genomeSet for testing',
            'elements': dict()
        }
        for genome_ref in cls.genome_refs:
            testGS['elements'][genome_scinames[genome_ref]] = {'ref': genome_ref}
        obj_info = cls.wsClient.save_objects({
            'workspace': cls.ws_info[1],
            'objects': [
                {
                    'type': 'KBaseSearch.GenomeSet',
                    'data': testGS,
                    'name': 'test_genomeset_1',
                    'meta': {},
                    'provenance': [
                        {
                            'service': 'kb_Msuite',
                            'method': 'test_CheckM'
                        }
                    ]
                }]
            })[0]
        cls.genomeSet_ref1 = str(obj_info[WSID_I]) + '/' + str(obj_info[OBJID_I]) + '/' + str(obj_info[VERSION_I])


#     def check_extended_result(self, result, link_name, file_names):
#         """
#         Test utility: check the file upload results for an extended report
#         Args:
#           result - result dictionary from running .create_extended_report
#           link_name - one of "html_links" or "file_links"
#           file_names - names of the files for us to check against
#         Returns:
#             obj - report object created
#         """
#         obj = self.check_created_report(result)
#         file_links = obj[link_name]
#         self.assertEqual(len(file_links), len(file_names))
#         # Test that all the filenames listed in the report object map correctly
#         saved_names = set([str(f['name']) for f in file_links])
#         self.assertEqual(saved_names, set(file_names))
#         return obj

    def check_validation_errors(self, params, error_list):

        """
        Check that the appropriate errors are thrown when validating extended report params
        Args:
          params - parameters to create_extended_report
          error_list - set of text regexes to check against the error string
        Returns True
        """
        err_str = 'KBaseReport parameter validation errors'
        with self.assertRaisesRegex(TypeError, err_str) as cm:
            self.getImpl().create_extended_report(self.getContext(), params)

        error_message = str(cm.exception)
        for e in error_list:
            self.assertRegex(error_message, e)

    def test_init_client(self):
        ''' check client initialisation '''

        # attr_list = ['callback_url', 'service_wizard_url', 'token', 'workspace_url']

        err_str = 'Missing required ClientUtil config value: callback_url'
        with self.assertRaisesRegex(ValueError, err_str):
            ClientUtil({})

        with self.assertRaisesRegex(ValueError, err_str):
            ClientUtil({'service_wizard_url': 'foo'})

        err_str = 'Missing required ClientUtil config value: token'
        with self.assertRaisesRegex(ValueError, err_str):
            ClientUtil({
                'callback_url': 'http://example.com',
                'service_wizard_url': 'http://example.com',
                'workspace_url': 'http://example.com',
            })

        cu = ClientUtil({
            'callback_url': 'http://example.com',
            'service_wizard_url': 'http://example.com',
            'workspace_url': 'http://example.com',
            'token': 'TOKEN!',
        })
        self.assertIsInstance(cu, ClientUtil)

        cmu = CheckMUtil(self.getConfig(), self.getContext())

        valid_clients = {
            'AssemblyUtil': AssemblyUtil,
            'MetagenomeUtils': MetagenomeUtils,
            'SetAPI': SetAPI,
            'Workspace': Workspace,
        }
        invalid_clients = ['FeatureSetUtils', 'TotallyMadeUpClient']

        for client in valid_clients.keys():
            self.assertFalse(hasattr(cmu.client_util, '_' + client))
            client_obj = cmu.client(client)
            self.assertTrue(hasattr(cmu.client_util, '_' + client))
            self.assertIsInstance(client_obj, valid_clients[client])

            # a bit of dirtiness to test that the same client is being returned
            setattr( getattr( cmu.client_util, '_' + client), '__seen', True)
            new_client_obj = cmu.client(client)
            self.assertTrue(hasattr(new_client_obj, '__seen'))

        for client in invalid_clients:
            err_str = client + ' client does not exist'
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.client(client)
            self.assertFalse(hasattr(cmu.client_util, '_' + client))


    def run_and_check_report(self, params, expected=None, with_filters=False):

        """
        Test utility: check the file upload results for an extended report
        Args:
          result - result dictionary from running .create_extended_report
          link_name - one of "html_links" or "file_links"
          file_names - names of the files for us to check against
        Returns:
            obj - report object created
        """
#         obj = self.check_created_report(result)
#         file_links = obj[link_name]
#
#         self.assertEqual(len(file_links), len(file_names))
#         # Test that all the filenames listed in the report object map correctly
#         saved_names = set([str(f['name']) for f in file_links])
#         self.assertEqual(saved_names, set(file_names))
#         return obj

        print("Running run_and_check_report")
        print(self)
        print(params)

        if not expected:
            expected = {
                'direct_html_link_index': 0,
                'n_file_links': 3,
                'n_html_links': 1,
                'html_links_0_name': 'checkm_results.html',
            }

        if (with_filters):
            result = self.getImpl().run_checkM_lineage_wf_withFilter(self.getContext(), params)[0]
        else:
            result = self.getImpl().run_checkM_lineage_wf(self.getContext(), params)[0]

        pprint('End to end test result:')
        pprint(result)

        self.assertIn('report_name', result)
        self.assertIn('report_ref', result)

        # make sure the report was created and includes the HTML report and download links
        rep = self.getWsClient().get_objects2({
            'objects': [{'ref': result['report_ref']}]
        })['data'][0]['data']

        print(rep)

        self.assertEqual(rep['direct_html_link_index'], 0)
        self.assertEqual(len(rep['file_links']), 3)
        self.assertEqual(len(rep['html_links']), 1)
        self.assertEqual(rep['html_links'][0]['name'], 'checkm_results.html')

    # Test 1: single assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_assembly")
    def test_checkM_lineage_wf_full_app_single_assembly(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_single_assembly")
        print("=================================================================\n")

        # run checkM lineage_wf app on a single assembly
        input_ref = self.assembly_OK_ref
        params = {
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 0,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'n_file_links': 3,
            'n_html_links': 1,
            'html_links_0_name': 'checkm_results.html',
        }

        self.run_and_check_report(params, expected_results)

    # Test 2: Regression test (CheckM <= v1.0.7) for single problem assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_problem_assembly")
    def test_checkM_lineage_wf_full_app_single_problem_assembly(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_single_problem_assembly")
        print("=================================================================\n")

        # run checkM lineage_wf app on a single assembly
        input_ref = self.assembly_dodgy_ref
        params = {
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,  # this must be 1 to regression test with --reduced_tree
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'n_file_links': 3,
            'n_html_links': 1,
            'html_links_0_name': 'checkm_results.html',
        }

        self.run_and_check_report(params, expected_results)

    # Test 3: binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_binned_contigs")
    def test_checkM_lineage_wf_full_app_binned_contigs(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_binned_contigs")
        print("=================================================================\n")

        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory

        # run checkM lineage_wf app on BinnedContigs
        input_ref = self.binned_contigs_ref
        params = {
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'n_file_links': 3,
            'n_html_links': 1,
            'html_links_0_name': 'checkm_results.html',
        }

        self.run_and_check_report(params, expected_results)

    # Test 4: Regression test for empty binned contigs object
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_binned_contigs_EMPTY")
    def test_checkM_lineage_wf_full_app_binned_contigs_EMPTY(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_binned_contigs_EMPTY")
        print("=================================================================\n")

        # run checkM lineage_wf app on EMPTY BinnedContigs
        input_ref = self.binned_contigs_empty_ref
        params = {
            'workspace_name': self.ws_info[1],
            'reduced_tree': 1,
            'input_ref': input_ref
        }
        with self.assertRaises(ValueError) as exception_context:
            self.getImpl().run_checkM_lineage_wf(self.getContext(), params)
        self.assertTrue('Binned Assembly is empty' in str(exception_context.exception))

    # Test 5: Assembly Set
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_assemblySet")
    def test_checkM_lineage_wf_full_app_assemblySet(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_assemblySet")
        print("=================================================================\n")

        # run checkM lineage_wf app on an assembly set
        input_ref = self.assemblySet_ref
        params = {
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'n_file_links': 3,
            'n_html_links': 1,
            'html_links_0_name': 'checkm_results.html',
        }

        self.run_and_check_report(params, expected_results)

    # Test 6: Single Genome
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_genome")
    def test_checkM_lineage_wf_full_app_single_genome(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_single_genome")
        print("=================================================================\n")

        # run checkM lineage_wf app on a single genome
        input_ref = self.genome_refs[0]
        params = {
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }
        expected_results = {
            'direct_html_link_index': 0,
            'n_file_links': 3,
            'n_html_links': 1,
            'html_links_0_name': 'checkm_results.html',
        }

        self.run_and_check_report(params, expected_results)

    # Test 7: Genome Set
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_genomeSet")
    def test_checkM_lineage_wf_full_app_genomeSet(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_genomeSet")
        print("=================================================================\n")

        # run checkM lineage_wf app on a genome set
        input_ref = self.genomeSet_ref1
        params = {
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }
        expected_results = {
            'direct_html_link_index': 0,
            'n_file_links': 3,
            'n_html_links': 1,
            'html_links_0_name': 'checkm_results.html',
        }

        self.run_and_check_report(params, expected_results)

    # Test 11: filter binned contigs to HQ binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_filter_binned_contigs")
    def test_checkM_lineage_wf_withFilter_binned_contigs(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_withFilter_binned_contigs")
        print("=================================================================\n")

        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory
        # run checkM lineage_wf app on BinnedContigs
        input_ref = self.binned_contigs_ref
        params = {
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'completeness_perc': 95.0,
            'contamination_perc': 1.5,
            'output_filtered_binnedcontigs_obj_name': 'filter.BinnedContigs',
            'threads': 4
        }
#        result = self.getImpl().run_checkM_lineage_wf_withFilter(self.getContext(), params)[0]

        expected_results = {
            'direct_html_link_index': 0,
            'n_file_links': 3,
            'n_html_links': 1,
            'html_links_0_name': 'checkm_results.html',
        }

        self.run_and_check_report(params, expected_results, True)

    # Test 8: Data staging (intended data not checked into git repo: SKIP)
    #
    # Uncomment to skip this test
    @unittest.skip("skipped test_data_staging")
    # missing test data for this custom test
    # note that the DataStagingUtils interface has not been updated below since the test is skipped
    def test_data_staging(self):

        # test stage assembly
        dsu = DataStagingUtils(self.cfg, self.ctx)
        staged_input = dsu.stage_input(self.assembly_OK_ref, 'strange_fasta_extension')
        pprint(staged_input)

        self.assertTrue(os.path.isdir(staged_input['input_dir']))
        self.assertTrue(os.path.isfile(staged_input['all_seq_fasta']))
        self.assertIn('folder_suffix', staged_input)

        self.assertTrue(os.path.isfile(os.path.join(staged_input['input_dir'],
                                                    'MyMetagenomeAssembly.strange_fasta_extension')))

        # test stage binned contigs
        staged_input2 = dsu.stage_input(self.binned_contigs_ref, 'fna')
        pprint(staged_input2)

        self.assertTrue(os.path.isdir(staged_input2['input_dir']))
        self.assertTrue(os.path.isfile(staged_input2['all_seq_fasta']))
        self.assertIn('folder_suffix', staged_input2)

        self.assertTrue(os.path.isfile(os.path.join(staged_input2['input_dir'],
                                                    'out_header.001.fna')))
        self.assertTrue(os.path.isfile(os.path.join(staged_input2['input_dir'],
                                                    'out_header.002.fna')))
        self.assertTrue(os.path.isfile(os.path.join(staged_input2['input_dir'],
                                                    'out_header.003.fna')))

    # Test 9: Plotting (intended data not checked into git repo: SKIP)
    #
    # Uncomment to skip this test
    @unittest.skip("skipped test_output_plotting")
    # missing test data for this custom test
    # note that the OutputBuilder interface has not been updated below since the test is skipped
    def test_output_plotting(self):
        impl = self.getImpl()
        cmu = CheckMUtil(impl)
        plots_dir = os.path.join(self.scratch, 'plots_1')
        html_dir = os.path.join(self.scratch, 'html_1')
        tetra_file = os.path.join(self.scratch, 'tetra_1.tsv')

        cmu.build_checkM_lineage_wf_plots(self.input_dir, self.output_dir, plots_dir,
                                          self.all_seq_fasta, tetra_file)
        self.assertTrue(os.path.isdir(plots_dir))
#        self.assertTrue(os.path.isfile(os.path.join(plots_dir, 'bin_qa_plot.png')))
        self.assertTrue(os.path.isfile(os.path.join(plots_dir, 'NewBins.001.ref_dist_plots.png')))
        self.assertTrue(os.path.isfile(os.path.join(plots_dir, 'NewBins.002.ref_dist_plots.png')))
        self.assertTrue(os.path.isfile(tetra_file))

        ob = OutputBuilder(self.output_dir, plots_dir, self.scratch, self.callback_url)
        os.makedirs(html_dir)
        res = ob.build_html_output_for_lineage_wf(html_dir, 'MyCheckMOutput')
        self.assertIn('shock_id', res)
        self.assertIn('name', res)
        self.assertIn('description', res)

        self.assertEqual(res['html_links'][0]['name'], self.getImpl().run_config['html_file'])

    # Test 10: tetra wiring (intended data not checked into git repo: SKIP)
    #
    # Uncomment to skip this test
    @unittest.skip("skipped test_checkM_local_function_wiring")
    # missing test data for this custom test
    def test_checkM_local_function_wiring(self):

        # run checkM lineage_wf app on a single assembly
        tetra_file = os.path.join(self.scratch, 'tetra_test.tsv')
        params = {
            'subcommand': 'tetra',
            'seq_file': self.all_seq_fasta,
            'tetra_file': tetra_file,
        }
        self.getImpl().run_checkM(self.getContext(), params)
        os.path.isfile(tetra_file)

    def setup_local_method_data(self):

        base_dir = os.path.dirname(__file__)
        test_data_dir = os.path.join(base_dir, 'data', 'example-bins')
        scratch_input_dir = os.path.join(self.scratch, 'lineage_wf_input_dir')
        scratch_output_dir = os.path.join(self.scratch, 'lineage_wf_output_dir'+'_'+str(self.suffix))
        shutil.copytree(test_data_dir, scratch_input_dir)
        if not os.path.exists(scratch_output_dir):
            os.mkdir(scratch_output_dir)
        log_path = os.path.join(self.scratch, 'lineage_wf.log')
        return scratch_input_dir, scratch_output_dir, log_path

    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_local_method()")
    def test_local_method(self):
        """
        Test a successful run of the .lineage_wf local method
        This just does some very basic testing to make sure the executable runs.
        """
        input_dir, output_dir, log_path = self.setup_local_method_data()
        self.getImpl().lineage_wf(self.getContext(), {
            'input_dir': input_dir,
            'output_dir': output_dir,
            'log_path': log_path,
            'options': {
                '-x': 'fasta',
                '--reduced_tree': ''
            }
        })
        out_contents = sorted(os.listdir(output_dir))
        # self.assertEqual(out_contents, ['storage', 'lineage.ms', 'bins'])
        self.assertEqual(out_contents, ['bins', 'checkm.log', 'lineage.ms', 'storage'])
        self.assertTrue(os.path.exists(log_path))
        # Remove test data
        os.remove(log_path)
        shutil.rmtree(input_dir)
        shutil.rmtree(output_dir)
