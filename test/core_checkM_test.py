# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import shutil

from os import environ
from configparser import ConfigParser
from pprint import pprint  # noqa: F401

from pathlib import Path

from installed_clients.WorkspaceClient import Workspace
from installed_clients.AssemblyUtilClient import AssemblyUtil
from installed_clients.SetAPIServiceClient import SetAPI
from installed_clients.GenomeFileUtilClient import GenomeFileUtil
from installed_clients.MetagenomeUtilsClient import MetagenomeUtils
from installed_clients.KBaseReportClient import KBaseReport

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
        cls.appdir      = cls.cfg['appdir']

        cls.test_data_dir = os.path.join(cls.scratch, 'test_data')
#        shutil.rmtree(cls.test_data_dir, ignore_errors=True)
        os.makedirs(cls.test_data_dir, exist_ok=True)

        cls.suffix      = test_time_stamp
        cls.checkm_runner = CheckMUtil(cls.cfg, cls.ctx)

        cls.wsName  = "test_kb_Msuite_" + str(cls.suffix)
        cls.ws_info = cls.wsClient.create_workspace({'workspace': cls.wsName})
        cls.au      = AssemblyUtil(os.environ['SDK_CALLBACK_URL'])
        cls.gfu     = GenomeFileUtil(os.environ['SDK_CALLBACK_URL'], service_ver='dev')
        cls.mu      = MetagenomeUtils(os.environ['SDK_CALLBACK_URL'])
        cls.setAPI  = SetAPI(url=cls.cfg['srv-wiz-url'], token=cls.ctx['token'])
        cls.kr      = KBaseReport(os.environ['SDK_CALLBACK_URL'])

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
        # cls.prepare_data()
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

        self.prep_report()

        self.prep_binned_contigs()

        self.prep_genomes()


    def prep_assemblies(self):
        ''' prepare the assemblies and assembly set '''

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
            assembly_file_path = os.path.join(self.test_data_dir, assembly['path'])
            if not os.path.exists(assembly_file_path):
                shutil.copy(os.path.join("data", assembly['path']), assembly_file_path)
            saved_assembly = self.au.save_assembly_from_fasta({
                'file': {'path': assembly_file_path},
                'workspace_name': self.ws_info[1],
                'assembly_name': assembly['name'],
            })
            setattr(self, assembly['attr'], saved_assembly)
            pprint('Saved Assembly: ' + getattr(self, assembly['attr']))

        # create an AssemblySet
        assembly_items = [
            {'ref': self.assembly_OK_ref, 'label': 'assembly_1'},
            {'ref': self.assembly_dodgy_ref, 'label': 'assembly_2'}
        ]
        self.assembly_set_ref = self.setAPI.save_assembly_set_v1({
            'workspace_name': self.ws_info[1],
            'output_object_name': 'TEST_ASSEMBLY_SET',
            'data': {
                'description': 'test assembly set',
                'items': assembly_items,
            },
        })['set_ref']
        pprint('Saved AssemblySet: ' + getattr(self, 'assembly_set_ref'))

        return True

    def prep_binned_contigs(self):

        if not hasattr(self, 'assembly_OK_ref'):
            self.prep_assemblies()

        # some binned contigs
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
            binned_contigs_path = os.path.join(self.test_data_dir, bc['path'])
            if not os.path.exists(binned_contigs_path) or not os.path.exists(os.path.join(binned_contigs_path, 'out_header.summary')):
                shutil.rmtree(binned_contigs_path, ignore_errors=True)
                shutil.copytree(os.path.join("data", bc['path']), binned_contigs_path)

            saved_object = self.mu.file_to_binned_contigs({
                'file_directory': binned_contigs_path,
                'workspace_name': self.ws_info[1],
                'assembly_ref': self.assembly_OK_ref,
                'binned_contig_name': bc['name'],
            })

            setattr(self, bc['path'] + '_ref', saved_object['binned_contig_obj_ref'])
            pprint('Saved BinnedContigs: ' + getattr(self, bc['path'] + '_ref'))

        return True

    def prep_genomes(self):

        ''' add a couple of genomes and create a genome set '''

        [OBJID_I, NAME_I, TYPE_I, SAVE_DATE_I, VERSION_I, SAVED_BY_I, WSID_I,
         WORKSPACE_I, CHSUM_I, SIZE_I, META_I] = list(range(11))  # object_info tuple

        # upload a few genomes
        self.genome_refs = []
        genomes = ['GCF_000022285.1_ASM2228v1_genomic.gbff', 'GCF_001439985.1_wTPRE_1.0_genomic.gbff']
        for genome_filename in genomes:
            genome_file_path = os.path.join(self.test_data_dir, genome_filename)
            if not os.path.exists(genome_file_path):
                shutil.copy(os.path.join("data", "genomes", genome_filename), genome_file_path)

            self.genome_refs.append(self.gfu.genbank_to_genome({
                    'file': {'path': genome_file_path},
                    'workspace_name': self.ws_info[1],
                    'genome_name': genome_filename,
                    'generate_ids_if_needed': 1,
                })['genome_ref'])

        # create a genomeSet
        genome_scinames = dict()
        for genome_i, genome_ref in enumerate(self.genome_refs):
            genome_scinames[genome_ref] = 'Genus species str. ' + str(genome_i)

        testGS = {
            'description': 'genomeSet for testing',
            'elements': dict()
        }
        for genome_ref in self.genome_refs:
            testGS['elements'][genome_scinames[genome_ref]] = {'ref': genome_ref}

        obj_info = self.wsClient.save_objects({
            'workspace': self.ws_info[1],
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
        self.genome_set_ref = str(obj_info[WSID_I]) + '/' + str(obj_info[OBJID_I]) + '/' + str(obj_info[VERSION_I])

        return True

    def prep_report(self):
        ''' copy templates into the test data directory and create a KBaseReport ref '''

        # copy over the templates
        test_tmpl_dir = os.path.join(self.test_data_dir, 'templates')
        shutil.rmtree(test_tmpl_dir, ignore_errors=True)
        os.makedirs(test_tmpl_dir, exist_ok=True)

        for tmpl in ['dist_html_page.tt', 'checkM_table.tt']:
            tmpl_file = os.path.join(test_tmpl_dir, tmpl)
            if not os.path.exists(tmpl_file):
                old_loc = os.path.join(self.appdir, "templates", tmpl)
                shutil.copy(old_loc, tmpl_file)
                if not os.path.isfile(tmpl_file):
                    print("Crap! file " + tmpl_file + " can't be found")

        tmpl_arr = [
            {
                'name': 'dist_page.html',
                'template': {
                    'template_file': os.path.join(test_tmpl_dir, 'dist_html_page.tt'),
                    'template_data_json': json.dumps({
                        'bin_id': 'The Best Bin Ever',
                        'dist_plot_ext': '.my.fave.ext',
                    })
                },
            },
            {
                'name': 'index_page.html',
                'template': {
                    'template_file': os.path.join(test_tmpl_dir, 'checkM_table.tt'),
                },
            },
        ]

        # add a kbasereport object
        report_output = self.kr.create_extended_report({
            'workspace_name': self.wsName,
            'report_object_name': 'my_report',
            'direct_html_link_index': 0,
            'html_links': tmpl_arr,
        })
        self.report_ref = report_output['ref']

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

    def test_00_module_init(self):

        print("\n=================================================================")
        print("RUNNING 00_module_init")
        print("=================================================================\n")

        cmu = CheckMUtil(self.cfg, self.ctx)

        # run config not yet initialised
        self.assertFalse(hasattr(cmu, '_run_config'))

        run_config = cmu.run_config()
        self.assertIsNotNone(run_config)
        self.assertTrue(hasattr(cmu, '_run_config'))

        # ensure the other attributes are also populated
        self.assertTrue(hasattr(cmu, 'datastagingutils'))
        self.assertIsInstance(cmu.datastagingutils, DataStagingUtils)
        self.assertTrue(hasattr(cmu, 'outputbuilder'))
        self.assertIsInstance(cmu.outputbuilder, OutputBuilder)

        # ensure we can reset the run_config
        base_dir = run_config['base_dir']
        cmu._set_run_configuration({'dir_name': 'last_chance_directory'})
        self.assertRegex(cmu.run_config()['base_dir'], r'run___last_chance_directory__\d+')
        self.assertNotEqual(base_dir, cmu.run_config()['base_dir'])

#   return re.sub('^[^\.]+\.', '', bin_id.replace('.' + fasta_ext, ''))
    def test_00_clean_bin_id(self):

        cmu = CheckMUtil(self.cfg, self.ctx)

        # bin_id = re.sub('^[^\.]+\.', '', bid)
        bin_ids = [
            'bin.056.fasta',
            'assembly.fasta',
            '../../this.is.fake',
        ]

        for bid in bin_ids:
            print(cmu.clean_bin_ID(bid, 'fasta'))

    def test_00_init_client(self):

        ''' check client initialisation '''
        print("\n=================================================================")
        print("RUNNING 00_init_client")
        print("=================================================================\n")

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

        print("Running run_and_check_report")

        if (with_filters):
            result = self.getImpl().run_checkM_lineage_wf_withFilter(self.getContext(), params)[0]
        else:
            result = self.getImpl().run_checkM_lineage_wf(self.getContext(), params)[0]

        return check_report(result, expected)

    def check_report(self, result, expected):

        pprint('End to end test result:')
        pprint(result)

        self.assertIn('report_name', result)
        self.assertIn('report_ref', result)

        # make sure the report was created and includes the HTML report and download links
        got_object = self.getWsClient().get_objects2({
            'objects': [{'ref': result['report_ref']}]
        })
        print(got_object)
        rep = got_object['data'][0]['data']
        print("\n\nreport data:")
        print(rep)
        print("\n\n")

        report_data = {
            'text_message': None,
            'file_links': [],
            'html_links': [],
            'warnings': [],
            'direct_html': None,
            'direct_html_link_index': None,
            'objects_created': [],
            'html_window_height': None,
            'summary_window_height': None,
        }

        report_data.update(expected)

        # expect the same keys in both
        # self.assertEqual(set(rep.keys()), set(expected.keys()))

#         for key in expected.keys():
#             with subTest('checking ' + key):
#                 if key == 'file_links' or key == 'html_links':
#                     self.check_report_links(rep, type, report_data)
#                 else:
#                     self.assertEqual(rep[key], report_data[key])

        return True

    def check_report_links(self, report_obj, type, expected):
        """
        Test utility: check the file upload results for an extended report
        Args:
          report_obj    - result dictionary from running .create_extended_report
          type          - one of "html_links" or "file_links"
          file_names    - names of the files for us to check against
        """
        file_links = report_obj[type]
        self.assertEqual(len(file_links), len(expected[type]))
        # Test that all the filenames listed in the report object map correctly
        saved_names = set([str(f['name']) for f in file_links])
        self.assertEqual(saved_names, set(expected[type]))
        return True

    # Test 1: single assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_assembly")
    def notest_checkM_lineage_wf_full_app_single_assembly(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_single_assembly")
        print("=================================================================\n")

        if not hasattr(self, 'assembly_OK_ref'):
            self.prep_assemblies()

        # run checkM lineage_wf app on a single assembly
        input_ref = self.assembly_OK_ref
        params = {
            'dir_name': 'single_assembly',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 0,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'something.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 2: Regression test (CheckM <= v1.0.7) for single problem assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_problem_assembly")
    def notest_checkM_lineage_wf_full_app_single_problem_assembly(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_single_problem_assembly")
        print("=================================================================\n")

        if not hasattr(self, 'assembly_OK_ref'):
            self.prep_assemblies()

        # run checkM lineage_wf app on a single assembly
        input_ref = self.assembly_dodgy_ref
        params = {
            'dir_name': 'dodgy_assembly',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,  # this must be 1 to regression test with --reduced_tree
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'something.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 3: binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_binned_contigs")
    def notest_checkM_lineage_wf_full_app_binned_contigs(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_binned_contigs")
        print("=================================================================\n")

        if not hasattr(self, 'binned_contigs_ref'):
            self.prep_binned_contigs()

        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory

        # run checkM lineage_wf app on BinnedContigs
        input_ref = self.binned_contigs_ref
        params = {
            'dir_name': 'binned_contigs',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'out_header.001.html', 'out_header.002.html', 'out_header.003.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 4: Regression test for empty binned contigs object
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_binned_contigs_EMPTY")
    def notest_checkM_lineage_wf_full_app_binned_contigs_EMPTY(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_binned_contigs_EMPTY")
        print("=================================================================\n")

        if not hasattr(self, 'binned_contigs_ref'):
            self.prep_binned_contigs()

        # run checkM lineage_wf app on EMPTY BinnedContigs
        input_ref = self.binned_contigs_empty_ref
        params = {
            'dir_name': 'binned_contigs_empty',
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
    def notest_checkM_lineage_wf_full_app_assemblySet(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_assemblySet")
        print("=================================================================\n")

        if not hasattr(self, 'assembly_set_ref'):
            self.prep_assemblies()

        # run checkM lineage_wf app on an assembly set
        input_ref = self.assembly_set_ref
        params = {
            'dir_name': 'assembly_set',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'assembly_1.html', 'assembly_2.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 6: Single Genome
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_genome")
    def notest_checkM_lineage_wf_full_app_single_genome(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_single_genome")
        print("=================================================================\n")

        if not hasattr(self, 'genome_refs'):
            self.prep_genomes()

        # run checkM lineage_wf app on a single genome
        input_ref = self.genome_refs[0]
        params = {
            'dir_name': 'single_genome',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'genome.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 7: Genome Set
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_genomeSet")
    def notest_checkM_lineage_wf_full_app_genomeSet(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_full_app_genomeSet")
        print("=================================================================\n")

        if not hasattr(self, 'genome_set_ref'):
            self.prep_genomes()

        # run checkM lineage_wf app on a genome set
        input_ref = self.genome_set_ref
        params = {
            'dir_name': 'genome_set',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }
        # two genomes in the genome set
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'genome_1.html', 'genome_2.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 11: filter binned contigs to HQ binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_filter_binned_contigs")
    def notest_checkM_lineage_wf_withFilter_binned_contigs(self):
        print("\n=================================================================")
        print("RUNNING checkM_lineage_wf_withFilter_binned_contigs")
        print("=================================================================\n")

        if not hasattr(self, 'binned_contigs_ref'):
            self.prep_binned_contigs()

        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory
        # run checkM lineage_wf app on BinnedContigs
        input_ref = self.binned_contigs_ref
        params = {
            'dir_name': 'binned_contigs_filter',
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

        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'out_header.001.html', 'out_header.002.html', 'out_header.003.html'],
        }

        self.run_and_check_report(params, expected_results, True)

    # Test 8: Data staging (intended data not checked into git repo: SKIP)
    #
    # Uncomment to skip this test
    # @unittest.skip("skipped test_data_staging")
    # missing test data for this custom test
    # note that the DataStagingUtils interface has not been updated below since the test is skipped
    def notest_01_data_staging(self):

        print("\n=================================================================")
        print("RUNNING 01_data_staging")
        print("=================================================================\n")

        if not hasattr(self, 'report_ref'):
            self.prep_report()

        if not hasattr(self, 'binned_contigs_ref'):
            self.prep_binned_contigs()

        # test stage assembly
        cmu = CheckMUtil(self.cfg, self.ctx)
        # init the run_config
        cmu.run_config()
        dsu = cmu.datastagingutils

        err_msg = 'Cannot stage fasta file input directory from type: '
        with self.assertRaisesRegex(ValueError, err_msg):
            dsu.stage_input(self.report_ref)

        # test stage binned contigs
        staged_input = dsu.stage_input(self.binned_contigs_ref)
        pprint(staged_input)

        self.assertTrue(os.path.isdir(staged_input['input_dir']))
        self.assertTrue(os.path.isfile(staged_input['all_seq_fasta']))
        self.assertIn('folder_suffix', staged_input)

        self.assertTrue(os.path.isfile(os.path.join(staged_input['input_dir'],
                                                    'out_header.001.fna')))
        self.assertTrue(os.path.isfile(os.path.join(staged_input['input_dir'],
                                                    'out_header.002.fna')))
        self.assertTrue(os.path.isfile(os.path.join(staged_input['input_dir'],
                                                    'out_header.003.fna')))

        cmu.fasta_extension = 'strange_fasta_extension'
        # reset the run_config
        cmu._set_run_configuration()
        dsu = cmu.datastagingutils
        staged_input = dsu.stage_input(self.assembly_OK_ref)
        pprint(staged_input)
        self.assertTrue(os.path.isdir(staged_input['input_dir']))
        self.assertTrue(os.path.isfile(staged_input['all_seq_fasta']))
        self.assertIn('folder_suffix', staged_input)

        self.assertTrue(os.path.isfile(os.path.join(staged_input['input_dir'],
                                                    'Test.Assembly.strange_fasta_extension')))


    # Test 9: Plotting (intended data not checked into git repo: SKIP)
    #
    # Uncomment to skip this test
    # @unittest.skip("skipped test_output_plotting")
    # missing test data for this custom test
    # note that the OutputBuilder interface has not been updated below since the test is skipped
    def test_05_outputbuilder(self):

        print("\n=================================================================")
        print("RUNNING 05_outputbuilder")
        print("=================================================================\n")

        cmu = CheckMUtil(self.cfg, self.ctx)
        # init the run_config
        run_config = cmu.run_config()

        with self.subTest('No checkM output'):
            # no checkM output: no report
            os.makedirs(run_config['output_dir'])
            Path(os.path.join(run_config['output_dir'], 'checkm.log')).touch(exist_ok=True)
            params = {
                'workspace_name': self.ws_info[1],
                'save_plots_dir': 1,
            }
            report = cmu.outputbuilder.build_report(params)

            expected_results = {
                'file_links': ['full_output'],
                'text_message': 'CheckM did not produce any output.',
                'html_links': None,
                'direct_html_link_index': None,
                'objects_created': [],
            }
            self.check_report(report, expected_results)

        # lots of output:
        with self.subTest('lots of checkM output'):
            cmu = CheckMUtil(self.cfg, self.ctx)
            run_config = cmu.run_config()
            os.makedirs(run_config['output_dir'])

            shutil.copytree(os.path.join('data', 'many_results'), run_config['output_dir'])
            params = {
                'workspace_name': self.ws_info[1],
                'save_plots_dir': 1,
            }
            report = cmu.outputbuilder.build_report(params)

            expected_results = {
                'direct_html_link_index': 0,
                'file_links': ['full_output.zip', 'CheckM_summary_table.tsv', 'plots', 'plots.zip' 'full_output'],
                'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'out_header.001.html', 'out_header.002.html', 'out_header.003.html'],
            }

            self.check_report(report, expected_results)

#         rerun with filters
#         cmu = CheckMUtil(self.cfg, self.ctx)
#         run_config = cmu.run_config()


        # empty output dir
#         if not os.path.isfile(stats_file):
#             log('Warning! no stats file found (looking at: ' + stats_file + ')')
#             return bin_stats


        # all items present and correct


#         impl = self.getImpl()
#         cmu = CheckMUtil(impl)
#         plots_dir = os.path.join(self.scratch, 'plots_1')
#         html_dir = os.path.join(self.scratch, 'html_1')
#         tetra_file = os.path.join(self.scratch, 'tetra_1.tsv')
#
#         cmu.build_checkM_lineage_wf_plots(self.input_dir, self.output_dir, plots_dir,
#                                           self.all_seq_fasta, tetra_file)
#         self.assertTrue(os.path.isdir(plots_dir))
# #        self.assertTrue(os.path.isfile(os.path.join(plots_dir, 'bin_qa_plot.png')))
#         self.assertTrue(os.path.isfile(os.path.join(plots_dir, 'NewBins.001.ref_dist_plots.png')))
#         self.assertTrue(os.path.isfile(os.path.join(plots_dir, 'NewBins.002.ref_dist_plots.png')))
#         self.assertTrue(os.path.isfile(tetra_file))
#
#         ob = OutputBuilder(self.output_dir, plots_dir, self.scratch, self.callback_url)
#         os.makedirs(html_dir)
#         res = ob.build_html_output_for_lineage_wf(html_dir, 'MyCheckMOutput')
#         self.assertIn('shock_id', res)
#         self.assertIn('name', res)
#         self.assertIn('description', res)
#
#         self.assertEqual(res['html_links'][0]['name'], self.getImpl().run_config['html_file'])

    def notest_02_filter_binned_contigs(self):

        print("\n=================================================================")
        print("RUNNING 02_filter_binned_contigs")
        print("=================================================================\n")

        if not hasattr(self, 'report_ref'):
            self.prep_report()

        if not hasattr(self, 'binned_contigs_ref'):
            self.prep_binned_contigs()

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu._set_run_configuration({'input_ref': self.report_ref})

        # wrong type
        self.assertIsNone(cmu._filter_binned_contigs({'input_ref': self.report_ref}))
        self.assertIsNone(cmu._filter_binned_contigs({'input_ref': self.assembly_dodgy_ref}))

        # no output_filtered_binnedcontigs_obj_name
        self.assertIsNone(cmu._filter_binned_contigs({'input_ref': self.binned_contigs_ref}))

        # empty input dir
        os.makedirs(cmu.run_config()['input_dir'], exist_ok=True)
        self.assertIsNone(cmu._filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Robin',
        }))

        with self.subTest('missing IDs in checkM output'):
            output_dir = cmu.run_config()['output_dir']
            os.makedirs(os.path.join(output_dir, 'storage'), exist_ok=True)

            # copy over a results file
            shutil.copy(os.path.join('data', 'filter_all_fail.bin_stats_ext.tsv'),
                os.path.join(output_dir, 'storage', 'bin_stats_ext.tsv')
            )

            for bid in list(range(5)):
                bid_path = os.path.join(output_dir, 'bins', 'out_header.00' + str(bid))
                os.makedirs(bid_path, exist_ok=True)
                Path(os.path.join(bid_path, 'genes.faa')).touch(exist_ok=True)

            missing_ids = ['out_header.000', 'out_header.004']
            err_str = "The following Bin IDs are missing from the checkM output: " + ", ".join(missing_ids)
            with self.assertRaisesRegex(ValueError, err_str):
                cmu._filter_binned_contigs({
                    'input_ref': self.binned_contigs_ref,
                    'output_filtered_binnedcontigs_obj_name': 'Robin',
                })

        with self.subTest('No HQ bins'):

            # delete the two interloper directories
            for bid in missing_ids:
                os.rmtree(os.path.join(output_dir, 'bins', bid), exist_ok=True)

            # no high quality bins
            self.assertIsNone(cmu._filter_binned_contigs({
                'input_ref': self.binned_contigs_ref,
                'output_filtered_binnedcontigs_obj_name': 'Robin',
                'completeness_perc': 99.0,
                'contamination_perc': 1.0,
            }))
            # no summary file
            self.assertFalse(os.path.exists(run_config['summary_file_path']))
            self.assertTrue(hasattr(cmu, 'bin_stats_data'))

        with self.subTest('some HQ bins'):
            # 001 and 002 will pass
            contig_filtering_results = cmu._filter_binned_contigs({
                'input_ref': self.binned_contigs_ref,
                'output_filtered_binnedcontigs_obj_name': 'Robin',
                'completeness_perc': 95.0,
                'contamination_perc': 1.5,
            })

            self.assertEqual(contig_filtering_results['filtered_object_name'], 'Robin')
            self.assertEqual(
                sorted(contig_filtering_results['retained_bin_IDs'].keys()),
                ['out_header.001', 'out_header.002']
            )
            self.assertEqual(
                sorted(contig_filtering_results['removed_bin_IDs'].keys()),
                ['out_header.003']
            )
            self.assertTrue('filtered_obj_ref' in contig_filtering_results)
            # summary file has been created
            self.assertTrue(os.path.exists(run_config['summary_file_path']))
            self.assertTrue(hasattr(cmu, 'bin_stats_data'))

        with self.subTest('All HQ bins'):
            # remove the summary file and re-filter so all pass
            os.remove(run_config['summary_file_path'])
            contig_filtering_results = cmu._filter_binned_contigs({
                'input_ref': self.binned_contigs_ref,
                'output_filtered_binnedcontigs_obj_name': 'Octocat',
                'completeness_perc': 95.0,
                'contamination_perc': 2.0,
            })
            self.assertEqual(contig_filtering_results['filtered_object_name'], 'Octocat')
            self.assertEqual(
                sorted(contig_filtering_results['retained_bin_IDs'].keys()),
                ['out_header.001', 'out_header.002', 'out_header.003']
            )
            self.assertEqual(
                sorted(contig_filtering_results['removed_bin_IDs'].keys()),
                []
            )
            self.assertTrue('filtered_obj_ref' in contig_filtering_results)
            # summary file has been created
            self.assertTrue(os.path.exists(run_config['summary_file_path']))
            self.assertTrue(hasattr(cmu, 'bin_stats_data'))


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
    def test_03_local_method(self):
        print("\n=================================================================")
        print("RUNNING 03_local_method")
        print("=================================================================\n")

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

