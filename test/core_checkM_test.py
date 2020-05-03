# -*- coding: utf-8 -*-
import unittest
import os  # noqa: F401
import json  # noqa: F401
import time
import shutil
import csv

from os import environ
from configparser import ConfigParser

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
from kb_Msuite.Utils.WorkspaceHelper import WorkspaceHelper
from kb_Msuite.Utils.BinnedContigFilter import BinnedContigFilter
from kb_Msuite.Utils.Utils import LogMixin


def print_method_name(method):
    def wrapper(*args, **kwargs):
        method_name = method.__name__
        method_name.replace("test_", "")
        self.logger.info("=================================================================")
        self.logger.info(("RUNNING " + method_name + "()"))
        self.logger.info("=================================================================\n")
        return method(*args, **kwargs)
    return wrapper


TEST_DATA = {

    'assembly_list': [
        {
            # example assembly
            'path': 'assembly.fasta',
            'name': 'Test.Assembly',
            'attr': 'assembly_OK_ref',
        }, {
            # contig that breaks checkm v1.0.7 reduced_tree (works on v1.0.8)
            'path': 'offending_contig_67815-67907.fa',
            'name': 'Dodgy_Contig.Assembly',
            'attr': 'assembly_dodgy_ref',
        }, {
            'path': 'mini_assembly.fasta',
            'name': 'MiniAssembly',
            'attr': 'assembly_mini_ref',
        }, {
            'attr': 'assembly_virus_ref',
            'name': 'Virus.Assembly.1KB',
            'path': 'GCF_002817975.1_ASM281797v1_genomic.fna',
        }, {
            'attr': 'assembly_a_ref',
            'name': 'Assembly.A.176KB',
            'path': 'GCF_001274515.1_ASM127451v1_genomic.fna',
        }, {
            'attr': 'assembly_b_ref',
            'name': 'Assembly.B.654KB',
            'path': 'GCF_005237295.1_ASM523729v1_genomic.fna',
    ],

    'assemblyset_list': [
        {
            'name': 'TEST_ASSEMBLY_SET',
            'items': [
                {'ref': 'something', 'label': 'assembly_1'},
                {'ref': 'something', 'label': 'assembly_2'}
            ],
            'attr': 'assembly_set_ref',
        }
    ],
    'genome_list': [
        {
            'path': 'GCF_000022285.1_ASM2228v1_genomic.gbff',

        },
        {
            'path': 'GCF_001439985.1_wTPRE_1.0_genomic.gbff',

        },{
            'path': 'GCF_002817975.1_ASM281797v1_genomic.gbff',
            'name': 'Virus.Genome.4KB',
            'attr': 'genome_virus_ref',
        }, {
            'path': 'GCF_001274515.1_ASM127451v1_genomic.gbff',
            'name': 'Genome.A.469KB',
            'attr': 'genome_a_ref',
        }, {
            'path': 'GCF_005237295.1_ASM523729v1_genomic.gbff',
            'name': 'Genome.B.1_6MB',
            'attr': 'genome_b_ref',
        }, {
    ],
    'genomeset_list': [
    ]
}


class CoreCheckMTest(unittest.TestCase, LogMixin):

    @classmethod
    def setUpClass(cls):
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
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = Workspace(cls.wsURL)
        cls.serviceImpl = kb_Msuite(cls.cfg)
        cls.callback_url = os.environ['SDK_CALLBACK_URL']
        cls.scratch = cls.cfg['scratch']
        cls.appdir = cls.cfg['appdir']

        cls.test_data_dir = os.path.join(cls.scratch, 'test_data')
        os.makedirs(cls.test_data_dir, exist_ok=True)

        cls.suffix = test_time_stamp
        cls.checkm_runner = CheckMUtil(cls.cfg, cls.ctx)

        cls.wsName = "test_kb_Msuite_" + str(cls.suffix)
        cls.ws_info = cls.wsClient.create_workspace({'workspace': cls.wsName})

        # refdata WS
        cls.refdata_wsName = 'test_kb_Msuite_refdata_1588183380977'
        cls.refdata_ws_info = [49697, 'test_kb_Msuite_refdata_1588183380977', 'ialarmedalien', '2020-04-29T18:03:01+0000', 0, 'a', 'n', 'unlocked', {}]

        cls.au = AssemblyUtil(os.environ['SDK_CALLBACK_URL'])
        cls.gfu = GenomeFileUtil(os.environ['SDK_CALLBACK_URL'], service_ver='dev')
        cls.mu = MetagenomeUtils(os.environ['SDK_CALLBACK_URL'])
        cls.setAPI = SetAPI(url=cls.cfg['srv-wiz-url'], token=cls.ctx['token'])
        cls.kr = KBaseReport(os.environ['SDK_CALLBACK_URL'])

        cls.data_loaded = False

        # stage an input and output directory
        """
        cls.input_dir = os.path.join(cls.scratch, 'input_1')
        cls.output_dir = os.path.join(cls.scratch, 'output_1')
        cls.all_seq_fasta = os.path.join(cls.scratch, 'all_seq.fna')
        shutil.copytree(os.path.join('data', 'example_out', 'input'), cls.input_dir)
        shutil.copytree(os.path.join('data', 'example_out', 'output'), cls.output_dir)
        shutil.copy(os.path.join('data', 'example_out', 'all_seq.fna'), cls.all_seq_fasta)
        """

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace ' + cls.wsName + ' was deleted')
        pass

    def require_data(self, *args):
        if self.data_loaded:
            return True

        return self.prep_ref_data()

    def prep_ref_data(self):

        saved_refs = {
            'assembly_OK_ref': '49697/1/1',
            'assembly_dodgy_ref': '49697/2/1',
            'assembly_set_ref': '49697/3/1',
            'binned_contigs_ref': '49697/4/1',
            'binned_contigs_empty_ref': '49697/5/1',
            'report_ref': '49697/6/1',
            'genome_refs': ['49697/8/1', '49697/10/1'],
            'genome_set_ref': '49697/11/1',
            'assembly_mini_ref': '49697/12/1',
            'binned_contigs_mini_ref': '49697/13/1',
            'assembly_virus_ref': '49697/14/1',
            'assembly_a_ref': '49697/15/1',,
            'assembly_b_ref': '49697/16/1',
            'genome_virus_ref': '49697/18/1',
            'genome_a_ref': '49697/20/1',
            'genome_b_ref': '49697/22/1',
        }

        for key, value in saved_refs.items():
            setattr(self, key, value)

            self.assertEqual(getattr(self, key), saved_refs[key])

        self.data_loaded = True
        return True

    def getWsClient(self):
        return self.__class__.wsClient

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def getConfig(self):
        return self.__class__.serviceImpl.config

    def _prep_assembly(self, assembly):
        '''
        input: dict of assembly data in the form
        {
            'path': '/path/to/assembly/file.fna',
            'name': 'Cool_Assembly_Name',
            'attr': 'assembly_blah_ref', # name of the attribute to populate
        }

        '''

        assembly_file_path = os.path.join(self.test_data_dir, "assemblies", assembly['path'])
        if not os.path.exists(assembly_file_path):
            shutil.copy(os.path.join("data", "assemblies", assembly['path']), assembly_file_path)

        saved_assembly = self.au.save_assembly_from_fasta({
            'file': {'path': assembly_file_path},
            'workspace_name': self.refdata_ws_info[1],
            'assembly_name': assembly['name'],
        })
        setattr(self, assembly['attr'], saved_assembly)
        self.logger.info({
            getattr(self, assembly['attr']): saved_assembly,
            assembly['attr']: saved_assembly,
        })

    def _prep_assemblyset(self, assemblyset):
        '''
        input: dict of assemblyset data in the form:
        {
            'name': 'Cool_AssemblySet_Name',
            'items': [{}]
            'attr': 'assemblyset_blah_ref',
        }

        '''
        saved_assembly_set = self.setAPI.save_assembly_set_v1({
            'workspace_name': self.refdata_ws_info[1],
            'output_object_name': assemblyset['name'],
            'data': {
                'description': 'test assembly set',
                'items': assemblyset['items'],
            },
        })
        setattr(self, assemblyset['attr'], saved_assembly_set['set_ref'])
        self.assembly_set_ref = saved_assembly_set['set_ref']
        self.logger.info({
            assemblyset['attr']: getattr(self, assemblyset['attr'],
            'Saved AssemblySet': saved_assembly_set,
        })

    def prep_assemblies(self):
        ''' prepare the assemblies and assembly set '''

        for assembly in TEST_DATA['assembly_list']:
            self._prep_assembly(assembly)

        assemblyset_list = [
            {
                'name': 'TEST_ASSEMBLY_SET',
                'items': [
                    {'ref': self.assembly_OK_ref, 'label': 'assembly_1'},
                    {'ref': self.assembly_dodgy_ref, 'label': 'assembly_2'}
                ],
                'attr': 'assembly_set_ref',
            }
        ]

        for assemblyset in assemblyset_list:
            self._prep_assemblyset(assemblyset)

        return True

    def _prep_binned_contig(self, bc):

        binned_contigs_path = os.path.join(self.test_data_dir, bc['path'])
        if not os.path.exists(binned_contigs_path) or not os.path.exists(os.path.join(binned_contigs_path, 'out_header.summary')):
            shutil.rmtree(binned_contigs_path, ignore_errors=True)
            shutil.copytree(os.path.join("data", bc['path']), binned_contigs_path)

        saved_object = self.mu.file_to_binned_contigs({
            'file_directory': binned_contigs_path,
            'workspace_name': self.refdata_ws_info[1],
            'assembly_ref': self.assembly_OK_ref,
            'binned_contig_name': bc['name'],
        })

        setattr(self, bc['path'] + '_ref', saved_object['binned_contig_obj_ref'])
        self.logger.info({
            'Saved BinnedContigs': saved_object,
            bc['path'] + '_ref': getattr(self, bc['path'] + '_ref')
        })

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
            },
            {
                'path': 'binned_contigs_mini',
                'name': 'Mini_Binned_Contigs',
            }
        ]

        for binned_contig in binned_contigs_list:
            self._prep_binned_contig(binned_contig)

        return True

    def _prep_genome(self, genome):

        genome_file_path = os.path.join(self.test_data_dir, genome['path'])
        if not os.path.exists(genome_file_path):
            shutil.copy(os.path.join("data", "genomes", genome['path']), genome_file_path)

        genome_data = self.gfu.genbank_to_genome({
            'file': {'path': genome_file_path},
            'workspace_name': self.refdata_ws_info[1],
            'genome_name': genome['name'],
            'generate_ids_if_needed': 1,
        })
        setattr(self, genome['attr'], genome_data['genome_ref'])
        self.logger.info({
            'Saved Genome': genome_data,
            getattr(self, genome['attr']): genome_data['genome_ref'],
        })

    def _prep_genomeset(self, genomeset):
        [OBJID_I, NAME_I, TYPE_I, SAVE_DATE_I, VERSION_I, SAVED_BY_I, WSID_I,
         WORKSPACE_I, CHSUM_I, SIZE_I, META_I] = list(range(11))  # object_info tuple

        obj_info = self.wsClient.save_objects({
            'workspace': self.refdata_ws_info[1],
            'objects': [
                {
                    'type': 'KBaseSearch.GenomeSet',
                    'data': genomeset['data'],
                    'name': genomeset['name'],
                    'meta': {},
                    'provenance': [
                        {
                            'service': 'kb_Msuite',
                            'method': 'test_CheckM'
                        }
                    ]
                }]
            })[0]
        reference = str(obj_info[WSID_I]) + '/' + str(obj_info[OBJID_I]) + '/' + str(obj_info[VERSION_I])
        setattr(self, genomeset['attr'], reference)

        self.logger.info({'Genome set ref': self.genome_set_ref})

    def prep_genomes(self):

        ''' add a couple of genomes and create a genome set '''

        # upload a few genomes
        self.genome_refs = []
        genomes = [{
            'path': 'GCF_001439985.1_wTPRE_1.0_genomic.gbff',
            'name': 'Genome.2_5MB',
            'attr': 'genome_d_ref',
        }, {
            'path': 'GCF_000022285.1_ASM2228v1_genomic.gbff',
            'name': 'Genome.3_4MB',
            'attr': 'genome_c_ref',
        }]

        for genome in genomes:
            self._prep_genome(genome)

        self.logger.info({'genome_refs': self.genome_refs})

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

        self._prep_genomeset({
            'name': 'test_genomeset_1',
            'attr': 'genome_set_ref',
            'data': testGS,
        })

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
                    self.logger.error("Crap! file " + tmpl_file + " can't be found")

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
            'workspace_name': self.refdata_ws_info[1],
            'report_object_name': 'my_report',
            'direct_html_link_index': 0,
            'html_links': tmpl_arr,
        })
        self.report_ref = report_output['ref']
        self.logger.info({'report_ref': self.report_ref})

        return True

    def run_and_check_report(self, params, expected=None, with_filters=False):

        if (with_filters):
            result = self.getImpl().run_checkM_lineage_wf_withFilter(self.getContext(), params)[0]
        else:
            result = self.getImpl().run_checkM_lineage_wf(self.getContext(), params)[0]

        return self.check_report(result, expected)

    def check_report(self, result, expected):

        self.assertIn('report_name', result)
        self.assertIn('report_ref', result)

        # make sure the report was created and includes the HTML report and download links
        got_object = self.getWsClient().get_objects2({
            'objects': [{'ref': result['report_ref']}]
        })
        rep = got_object['data'][0]['data']
        self.logger.info({'report data': rep})

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

        for key in expected.keys():
            with self.subTest('checking ' + key):
                if key == 'file_links' or key == 'html_links':
                    self.check_report_links(rep, key, report_data)
                else:
                    self.assertEqual(rep[key], report_data[key])

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

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 00_module_init")
        self.logger.info("=================================================================\n")

        self.prep_ref_data()

        new_assemblies = [
        {
            'attr': 'assembly_empty_ref',
            'name': 'Assembly.Empty',
            'path': 'empty_assembly.fasta',
        }]
        for assembly in new_assemblies:
            self._prep_assembly(assembly)

       new_genomes = [{
            'path': 'empty_genomic.gbff',
            'name': 'Empty_Genome',
            'attr': 'genome_empty_ref',
        }]
        for genome in new_genomes:
            self._prep_genome(genome)
        cmu = CheckMUtil(self.cfg, self.ctx)
        # run config not yet initialised
        self.assertFalse(hasattr(cmu, '_run_config'))

        run_config = cmu.run_config()
        self.assertIsNotNone(run_config)
        self.assertTrue(hasattr(cmu, '_run_config'))

        # ensure the other attributes are also populated
        obj_name_to_type = {
            'datastagingutils': DataStagingUtils,
            'outputbuilder': OutputBuilder,
            'workspacehelper': WorkspaceHelper,
            'binnedcontigfilter': BinnedContigFilter,
        }
        for attr, type in obj_name_to_type.items():
            self.assertTrue(hasattr(cmu, attr))
            self.assertIsInstance(getattr(cmu, attr), type)

        # ensure we can reset the run_config
        base_dir = run_config['base_dir']
        cmu._set_run_config({'dir_name': 'last_chance_directory'})
        self.assertRegex(cmu.run_config()['base_dir'], r'run___last_chance_directory__\d+')
        self.assertNotEqual(base_dir, cmu.run_config()['base_dir'])

#   return re.sub('^[^\.]+\.', '', bin_id.replace('.' + fasta_ext, ''))
    def notest00_clean_bin_id(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING 00_clean_bin_id")
        self.logger.info("=================================================================\n")

        cmu = CheckMUtil(self.cfg, self.ctx)

        expected = [
            ['bin.056.fasta', 'bin.056', 'bin.056.fasta'],
            ['assembly.fasta', 'assembly', 'assembly.fasta'],  # super dodgy!
            ['../../this.is.fake', '../../this.is.fake', '../../this.is.fake'],
        ]

        for bid in expected:
            self.assertEqual(cmu.clean_bin_ID(bid[0], 'fasta'), bid[1])
            self.logger.info(cmu.clean_bin_ID(bid[0]), bid[2])

    def notest00_workspace_helper(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 00_workspace_helper")
        self.logger.info("=================================================================\n")

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.run_config()

        # create a report
        report_object_name = 'Super_Cool_Extended_Report'
        text_message = 'This is the best report in the world'
        report_output = self.kr.create_extended_report({
            'workspace_name': self.ws_info[1],
            'report_object_name': report_object_name,
            'message': text_message,
        })
        self.logger.info(report_output)
        # {'ref': '49674/1/1', 'name': 'Super_Cool_Extended_Report'}

        ws_obj_info = cmu.workspacehelper.get_ws_obj_info(report_output['ref'])
        self.logger.debug(ws_obj_info)

        with self.subTest('get_object_property'):
            obj_name = cmu.workspacehelper.get_object_property(ws_obj_info, 'name')
            self.assertEqual(obj_name, report_object_name)

            obj_type = cmu.workspacehelper.get_object_property(ws_obj_info, 'type')
            self.assertEqual(obj_type, 'KBaseReport.Report-3.0')

            err_str = 'personality is not a valid workspace object property'
            with self.assertRaisesRegex(KeyError, err_str):
                cmu.workspacehelper.get_object_property(ws_obj_info, 'personality')

        with self.subTest('get_ws_obj type and name'):
            obj_name = cmu.workspacehelper.get_ws_obj_name(report_output['ref'])
            self.assertEqual(obj_name, report_object_name)

            obj_type = cmu.workspacehelper.get_ws_obj_name(object_info=ws_obj_info)
            self.assertEqual(obj_type, report_object_name)

            err_str = "Must supply either ref or object_info to get_ws_obj_name"
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.workspacehelper.get_ws_obj_name()

            err_str = "Must supply either ref or object_info to get_ws_obj_type"
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.workspacehelper.get_ws_obj_type(remove_module=True)

            obj_type = cmu.workspacehelper.get_ws_obj_type(report_output['ref'])
            self.assertEqual(obj_type, 'KBaseReport.Report')

            obj_type = cmu.workspacehelper.get_ws_obj_type(object_info=ws_obj_info, remove_module=False)
            self.assertEqual(obj_type, 'KBaseReport.Report')

            obj_type = cmu.workspacehelper.get_ws_obj_type(ref=report_output['ref'], remove_module=True)
            self.assertEqual(obj_type, 'Report')

            obj_type = cmu.workspacehelper.get_ws_obj_type(object_info=ws_obj_info, remove_module=True)
            self.assertEqual(obj_type, 'Report')

        with self.subTest('get obj from workspace'):
            ws_obj = cmu.workspacehelper.get_obj_from_workspace(report_output['ref'])
            self.logger.info(ws_obj)
            self.assertEqual(ws_obj['text_message'], text_message)

            err_str = 'Unable to fetch ROTFLMAO object from workspace:'
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.workspacehelper.get_obj_from_workspace('ROTFLMAO')

    def notest00_init_client(self):

        ''' check client initialisation '''
        self.logger.info("=================================================================")
        self.logger.info("RUNNING 00_init_client")
        self.logger.info("=================================================================\n")

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
            setattr(getattr(cmu.client_util, '_' + client), '__seen', True)
            new_client_obj = cmu.client(client)
            self.assertTrue(hasattr(new_client_obj, '__seen'))

        for client in invalid_clients:
            err_str = client + ' client does not exist'
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.client(client)
            self.assertFalse(hasattr(cmu.client_util, '_' + client))

#     Standard Single Assembly
#     'KBaseGenomeAnnotations.Assembly': self.process_assembly_contigset,
#     'KBaseGenomes.ContigSet': self.process_assembly_contigset, --- TODO
#     AssemblySet
#     'KBaseSets.AssemblySet': self.process_assembly_set,
#     Binned Contigs
#     'KBaseMetagenomes.BinnedContigs': self.process_binned_contigs,
#     Genome and GenomeSet
#     'KBaseGenomes.Genome': self.process_genome_genome_set,
#     'KBaseSearch.GenomeSet': self.process_genome_genome_set,
#   also test:
#   - empty versions of each of these

    def notest01_data_staging(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_data_staging")
        self.logger.info("=================================================================\n")

        self.require_data('report_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        # init the run_config
        cmu.run_config()

        with self.subTest('erroneous report object staging'):
            err_msg = 'Cannot stage fasta file input directory from type: '
            with self.assertRaisesRegex(ValueError, err_msg):
                cmu.datastagingutils.stage_input(self.report_ref)

    def check_data_staging_results(self, run_config, filenames):

        self.assertTrue(os.path.isdir(run_config['input_dir']))
        self.assertTrue(os.path.isfile(run_config['all_seq_fasta']))
        for name in filenames:
            self.assertTrue(os.path.isfile(os.path.join(
                run_config['input_dir'], name + '.' + run_config['fasta_ext'])
            ))

    def notest01_data_staging_assembly(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_data_staging_assembly")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_mini_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu.run_config()
        dsu = cmu.datastagingutils
        staged_input = dsu.stage_input(self.assembly_mini_ref)
        self.assertEqual(
            staged_input,
            {'obj_name': 'MiniAssembly', 'obj_type': 'KBaseGenomeAnnotations.Assembly'}
        )
        self.check_data_staging_results(run_config, ['MiniAssembly'])

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    # def notest01_data_staging_things_are_empty(self):

    #     self.require_data('assembly_empty', 'assembly_set_empty',
    #         'genome_empty', 'genome_set_empty')

    #     cmu = CheckMUtil(self.cfg, self.ctx)
    #     cmu.run_config()
    #     dsu = cmu.datastagingutils

    #     err_str = 'Assembly or ContigSet is empty in filename: '
    #     with self.assertRaisesRegex(ValueError, err_str):
    #         dsu.stage_input(self.assembly_empty)

        # assemblyset errors:

    #    raise ValueError('Unable to get object from workspace: (' + input_ref + ')' + str(e))
        # with self.assertRaisesRegex(ValueError, err_str):
        #     dsu.stage_input(self.ref_does_not_exist)

        # error fetching one of the assemblyset constituents from the WS
        # raise ValueError('Unable to get object from workspace: (' + assembly_ref + '): ' + str(e))

        # assembly util problem:
        # raise ValueError('Error generating fasta file from an Assembly or ContigSet with AssemblyUtil')

        # one of the assemblies is empty:
        # with self.assertRaisesRegex(ValueError, err_str):
        #     dsu.stage_input(self.assembly_empty)
        #         raise ValueError('Assembly or ContigSet is empty in filename: ' + str(filename))

        # empty genome / genome set
        # raise ValueError('genome_ref not found for genome_id: ' + str(genome_id) + ' in genomeSet: ' + str(input_ref))
        # msg = "Genome "+genome_obj_names[i]+" (ref:"+input_ref+") "+genome_sci_names[i]+" MISSING BOTH contigset_ref AND assembly_ref. Cannot process. Exiting."
        # raise ValueError('Assembly or ContigSet is empty in filename: '+str(filename))

    def notest01_data_staging_binned_contigs_empty(self):

        self.require_data('binned_contigs_empty_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.run_config()
        dsu = cmu.datastagingutils

        err_str = 'Binned Assembly is empty for fasta_path: '
        with self.assertRaisesRegex(ValueError, err_str):
            dsu.stage_input(self.binned_contigs_empty_ref)

    def notest01_data_staging_assembly_strange_fasta_ext(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_data_staging_assembly_strange_fasta_ext")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_OK_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.fasta_extension = 'strange_fasta_extension'
        run_config = cmu._set_run_config()
        dsu = cmu.datastagingutils
        staged_input = dsu.stage_input(self.assembly_OK_ref)
        self.assertEqual(
            staged_input,
            {'obj_name': 'Test.Assembly', 'obj_type': 'KBaseGenomeAnnotations.Assembly'}
        )
        self.check_data_staging_results(run_config, ['Test.Assembly'])

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def notest01_data_staging_assemblyset(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_data_staging_assemblyset")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_set_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu.run_config()
        dsu = cmu.datastagingutils
        staged_input = dsu.stage_input(self.assembly_set_ref)
        self.assertEqual(
            staged_input,
            {
                'obj_name': 'TEST_ASSEMBLY_SET',
                'obj_type': 'KBaseSets.AssemblySet',
            }
        )
        self.check_data_staging_results(run_config, ['Test.Assembly', 'Dodgy_Contig.Assembly'])

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def notest01_data_staging_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_data_staging_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.run_config()
        dsu = cmu.datastagingutils

        # test stage binned contigs
        staged_input = dsu.stage_input(self.binned_contigs_ref)
        run_config = dsu.run_config()
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
                run_config['input_dir'], 'out_header.00' + number + '.' + run_config['fasta_ext']
            )))

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def notest01_data_staging_genome(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_data_staging_genome")
        self.logger.info("=================================================================\n")

        self.require_data('genome_refs')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.fasta_extension = 'strange_fasta_extension'
        run_config = cmu._set_run_config()
        dsu = cmu.datastagingutils

        staged_input = dsu.stage_input(self.genome_refs[1])
        self.assertEqual(
            staged_input,
            {
                'obj_name': 'GCF_001439985.1_wTPRE_1.0_genomic.gbff',
                'obj_type': 'KBaseGenomes.Genome',
            }
        )
        self.check_data_staging_results(run_config, ['GCF_001439985.1_wTPRE_1.0_genomic.gbff'])
        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def notest01_data_staging_genome_set(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_data_staging_genome_set")
        self.logger.info("=================================================================\n")

        self.require_data('genome_set_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu._set_run_config()
        dsu = cmu.datastagingutils

        staged_input = dsu.stage_input(self.genome_set_ref)
        self.assertEqual(
            staged_input,
            {
                'obj_name': 'test_genomeset_1',
                'obj_type': 'KBaseSearch.GenomeSet',
            }
        )
        self.check_data_staging_results(run_config, [
            'GCF_000022285.1_ASM2228v1_genomic.gbff', 'GCF_001439985.1_wTPRE_1.0_genomic.gbff'
        ])

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def notest02_filter_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_filter_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref', 'report_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.run_config()

        # wrong type
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.report_ref}))
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.assembly_dodgy_ref}))

        # no output_filtered_binnedcontigs_obj_name
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.binned_contigs_ref}))

        # empty input dir
        os.makedirs(cmu.run_config()['input_dir'], exist_ok=True)
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Alpha',
        }))
        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def prep_filter_binned_contigs_dirs(self):

        self.require_data('binned_contigs_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.fasta_extension = 'fasta'
        cmu.run_config()
        run_config = cmu.run_config()

        # copy over a results file
        # out_header.001  'Completeness': 97.6,              'Contamination': 1.907,
        # out_header.002  'Completeness': 98.11542991755006, 'Contamination': 1.4134275618374559,
        # out_header.003  'Completeness': 96.34019795657727, 'Contamination': 1.7600574712643677,
        output_dir = run_config['output_dir']
        os.makedirs(os.path.join(output_dir, 'storage'), exist_ok=True)
        shutil.copy(os.path.join('data', 'filter_all_fail.bin_stats_ext.tsv'), run_config['bin_stats_ext_file'])

        os.makedirs(run_config['input_dir'], exist_ok=True)
        for bid in [1, 2, 3]:
            bid_path = os.path.join(
                run_config['input_dir'],
                'out_header.00' + str(bid) + '.' + run_config['fasta_ext']
            )
            Path(bid_path).touch(exist_ok=True)

        return cmu

    def notest02_filter_binned_contigs_checkM_missing_IDs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_filter_binned_contigs_checkM_missing_IDs")
        self.logger.info("=================================================================\n")

        cmu = self.prep_filter_binned_contigs_dirs()
        run_config = cmu.run_config()

        missing_ids = ['000', '004']
        for bid in missing_ids:
            bid_path = os.path.join(
                run_config['input_dir'],
                'out_header.' + str(bid) + '.' + run_config['fasta_ext']
            )
            Path(bid_path).touch(exist_ok=True)

        err_str = "The following Bin IDs are missing from the checkM output: " + ", ".join(missing_ids)
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.binnedcontigfilter.filter_binned_contigs({
                'input_ref': self.binned_contigs_ref,
                'output_filtered_binnedcontigs_obj_name': 'Beta',
            })

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def notest02_filter_binned_contigs_no_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_filter_binned_contigs_no_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.prep_filter_binned_contigs_dirs()
        run_config = cmu.run_config()
        # no high quality bins
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Gamma',
            'completeness_perc': 99.0,
            'contamination_perc': 1.0,
            'workspace_name': self.wsName,
        }))
        # no summary file
        self.assertFalse(os.path.exists(run_config['summary_file_path']))
        self.assertTrue(hasattr(cmu, 'bin_stats_data'))

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def notest02_filter_binned_contigs_all_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_filter_binned_contigs_all_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.prep_filter_binned_contigs_dirs()
        run_config = cmu.run_config()
        # set filters so all will pass => returns none
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Theta',
            'completeness_perc': 95.0,
            'contamination_perc': 2.0,
            'workspace_name': self.wsName,
        }))
        # no summary file
        self.assertFalse(os.path.exists(run_config['summary_file_path']))
        self.assertTrue(hasattr(cmu, 'bin_stats_data'))

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def check_filtered_bins(self, cmu, run_config, results, expected):

        for key, value in expected.items():
            self.assertEqual(results[key], value)

        self.assertTrue('filtered_obj_ref' in results)

        # summary file has been created
        self.assertTrue(os.path.exists(run_config['summary_file_path']))
        self.assertTrue(hasattr(cmu, 'bin_stats_data'))
        expected_content_file = os.path.join(
            'data', 'expected', expected['filtered_obj_name'] + '.summary'
        )
        # check that the summary file is as expected
        self.assertEqual(
                open(run_config['summary_file_path'], 'r').read(),
                open(expected_content_file, 'r').read()
            )

        # filtered bins dir has been created
        self.assertTrue(os.path.exists(run_config['filtered_bins_dir']))
        for id in expected['retained_bin_IDs'].keys():
            self.assertTrue(os.path.isfile(os.path.join(
                run_config['filtered_bins_dir'],
                run_config['bin_basename'] + '.' + id + '.' + run_config['fasta_ext']
            )))

        #

    def notest02_filter_binned_contigs_some_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_filter_binned_contigs_some_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.prep_filter_binned_contigs_dirs()
        run_config = cmu.run_config()
        # 002 will pass
        results = cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Epsilon',
            'completeness_perc': 95.0,  # all pass
            'contamination_perc': 1.5,  # 002 passes
            'workspace_name': self.wsName,
        })
        expected = {
            'filtered_obj_name': 'Epsilon',
            'retained_bin_IDs': {'002': True},
            'removed_bin_IDs': {'001': True, '003': True}
        }
        self.check_filtered_bins(cmu, run_config, results, expected)

    def notest02_filter_binned_contigs_some_others_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_filter_binned_contigs_some_others_HQ")
        self.logger.info("=================================================================\n")

        cmu = self.prep_filter_binned_contigs_dirs()
        run_config = cmu.run_config()
        # 001 and 002 will pass
        results = cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Gamma',
            'completeness_perc': 97.0,  # 001 and 002 pass
            'contamination_perc': 2.5,  # all pass
            'workspace_name': self.wsName,
        })
        expected = {
            'filtered_obj_name': 'Gamma',
            'retained_bin_IDs': {'001': True, '002': True},
            'removed_bin_IDs': {'003': True},
        }
        self.check_filtered_bins(cmu, run_config, results, expected)

#     def test_05_write_tsv_output(self):
#
#         cmu = CheckMUtil(self.cfg, self.ctx)
#         run_config = cmu.run_config()
#         ob = cmu.outputbuilder
#
#         # set up some plots
#
#         demo_data = self.demo_bin_stats()
#
#
#         with open(run_config['base_dir'], 'w', newline='') as tab_text_fh:
#             tsv_writer = self.init_tsv_writer(tab_text_fh)
#             self.write_tsv_headers(tsv_writer, results_filtered)
#             self.write_tsv_row(tsv_writer, bid, bin_stats[bid], results_filtered)
#
#         self.assertIsInstance

    def notest05_outputbuilder(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 05_outputbuilder")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu.run_config()

        # lots of output:
        with self.subTest('lots of checkM output'):
            shutil.rmtree(run_config['base_dir'], ignore_errors=True)
            os.makedirs(run_config['base_dir'], exist_ok=True)
            for dir in ['bins', 'output', 'plots']:
                shutil.copytree(
                    os.path.join('data', 'many_results', dir),
                    os.path.join(run_config['base_dir'], dir)
                )

            params = {
                'workspace_name': self.ws_info[1],
                'save_plots_dir': 1,
            }
            result = cmu.outputbuilder.build_report(params)
            self.assertEqual(set(result.keys()), set(['report_name', 'report_ref']))

            expected_results = {
                'direct_html_link_index': 0,
                'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
                'html_links': [
                    'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                    'bin002.html', 'bin005.html', 'bin006.html',
                    'bin009.html', 'bin014.html', 'bin033.html',
                ],
            }
            self.check_report(result, expected_results)
            # TODO: check the TSV file -- there should be one bin with a missing plot file
            with open(run_config['tab_text_file'], newline='') as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                for row in reader:
                    self.logger.debug({'row': row})

        with self.subTest('lots of output, binned contig obj'):

            for dir in ['html_dir', 'tab_text_dir', 'template_dest_dir']:
                shutil.rmtree(run_config[dir], ignore_errors=True)

            params = {
                'workspace_name': self.ws_info[1],
            }

            filtered_obj_info = {
                'filtered_obj_ref': self.binned_contigs_ref,
                'filtered_obj_name': 'Nancy Drew',
                'removed_bin_IDs': {'002': True, '005': True, '033': True},
            }

            result = cmu.outputbuilder.build_report(params, filtered_obj_info)
            self.assertEqual(set(result.keys()), set(['report_name', 'report_ref', 'binned_contig_obj_ref']))

            self.assertEqual(result['binned_contig_obj_ref'], self.binned_contigs_ref)
            expected_results = {
                'direct_html_link_index': 0,
                'file_links': ['CheckM_summary_table.tsv', 'full_output'],
                'html_links': [
                    'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                    'bin002.html', 'bin005.html', 'bin006.html',
                    'bin009.html', 'bin014.html', 'bin033.html',
                ],
                'objects_created': [{
                    'ref': filtered_obj_info['filtered_obj_ref'],
                    'description':  'HQ BinnedContigs ' + filtered_obj_info['filtered_obj_name'],
                }]
            }

            self.check_report(result, expected_results)
            # TODO: check the TSV file -- there should be one bin with a missing plot file
            with open(run_config['tab_text_file'], newline='') as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                for row in reader:
                    self.logger.debug({'row': row})

        shutil.rmtree(run_config['base_dir'])

    def notest05_outputbuilder_no_checkM_output(self):

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu.run_config()
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
        }
        self.check_report(report, expected_results)
        shutil.rmtree(run_config['base_dir'])

    # Test 1: single assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_assembly")
    def notest_checkM_lineage_wf_full_app_single_assembly(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_full_app_single_assembly")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_OK_ref')
        # run checkM lineage_wf app on a single assembly
        input_ref = self.assembly_mini_ref
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
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'MiniAssembly.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 2: Regression test (CheckM <= v1.0.7) for single problem assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_problem_assembly")
    def notest_checkM_lineage_wf_full_app_single_problem_assembly(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_full_app_single_problem_assembly")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_dodgy_ref')
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
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'something.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 3: binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_binned_contigs")
    def notest_checkM_lineage_wf_full_app_binned_contigs(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_full_app_binned_contigs")
        self.logger.info("=================================================================\n")

        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory

        self.require_data('binned_contigs_ref')
        # run checkM lineage_wf app on BinnedContigs
        input_ref = self.binned_contigs_mini_ref
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
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                '001.html', '002.html', '003.html'
            ],
        }

        self.run_and_check_report(params, expected_results)

    # Test 4: Regression test for empty binned contigs object
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_binned_contigs_EMPTY")
    def notest_checkM_lineage_wf_full_app_binned_contigs_EMPTY(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_full_app_binned_contigs_EMPTY")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_empty_ref')
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
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_full_app_assemblySet")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_set_ref')
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
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'assembly_1.html', 'assembly_2.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 6: Single Genome
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_single_genome")
    def notest_checkM_lineage_wf_full_app_single_genome(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_full_app_single_genome")
        self.logger.info("=================================================================\n")

        self.require_data('genome_refs')
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
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'GCF_0000222851_ASM2228v1_genomicgbff.html'],
        }
        # correct the file name!
        self.run_and_check_report(params, expected_results)

    # Test 7: Genome Set
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_genomeSet")
    def notest_checkM_lineage_wf_full_app_genomeSet(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_full_app_genomeSet")
        self.logger.info("=================================================================\n")

        self.require_data('genome_set_ref')
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
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': ['checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'genome_1.html', 'genome_2.html'],
        }

        self.run_and_check_report(params, expected_results)

    # Test 11: filter binned contigs to HQ binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_lineage_wf_full_app_filter_binned_contigs")
    def notest_checkM_lineage_wf_withFilter_binned_contigs(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_lineage_wf_withFilter_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref')

        # Even with the reduced_tree option, this will take a long time and crash if your
        # machine has less than ~16gb memory
        # run checkM lineage_wf app on BinnedContigs
        input_ref = self.binned_contigs_mini_ref
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
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                '001.html', '002.html', '003.html'
            ],
        }

        self.run_and_check_report(params, expected_results, True)

    # Test 10: tetra wiring (intended data not checked into git repo: SKIP)
    #
    # Uncomment to skip this test
    @unittest.skip("skipped test_checkM_local_function_wiring")
    # missing test data for this custom test
    def notest_checkM_local_function_wiring(self):

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
    def notest03_local_method(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_local_method")
        self.logger.info("=================================================================\n")

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

    def demo_bin_stats(self):
        return {
            'bin.002': {'GC std': 0.022204954407497902, '# genomes': 5449, 'Genome size': 605546, 'Longest contig': 7264, 'GCN0': ['PF00162', 'PF00298', 'PF00889', 'TIGR00615', 'PF03484', 'PF04983', 'PF01016', 'PF00562', 'PF00338', 'PF04563', 'PF01196', 'PF00281', 'PF00673', 'PF04997', 'PF00380', 'PF01245', 'PF10385', 'PF00411', 'TIGR00344', 'PF01281', 'PF00416', 'PF00831', 'TIGR00459', 'TIGR00755', 'PF04565', 'PF02367', 'PF11987', 'PF00312', 'PF13603', 'PF03948', 'PF00347', 'PF05000', 'PF00297', 'PF00366', 'TIGR02432', 'TIGR00460', 'PF00177', 'PF00828', 'PF03946', 'PF00886', 'TIGR03263', 'PF00333', 'PF00410', 'PF00164', 'TIGR00967', 'PF01193', 'PF01765', 'TIGR03594', 'PF08529', 'PF02130', 'PF04560', 'TIGR00922', 'TIGR02075', 'PF01000', 'TIGR00810', 'TIGR01079', 'PF01632', 'TIGR03723', 'PF00238', 'PF01121', 'PF01746', 'PF01195', 'PF00572', 'PF03719', 'PF00829', 'PF00861', 'PF00466', 'PF04998', 'PF06071', 'TIGR00855', 'TIGR00329', 'TIGR00084', 'TIGR00250', 'PF13184', 'PF04561', 'TIGR00019', 'PF00687', 'PF05697', 'PF00453', 'PF01250', 'PF08459', 'PF00318', 'PF05491', 'PF02978', 'PF00623'], 'GCN1': ['PF01509', 'PF01018', 'PF00189', 'PF01795', 'PF00252', 'TIGR00392', 'PF01668', 'PF00181', 'PF12344', 'PF01409', 'PF03947', 'PF00203', 'PF00237', 'PF00276', 'PF01649', 'PF06421', 'PF02912', 'PF02033', 'PF00573'], 'GCN2': [], 'marker lineage': 'k__Bacteria', 'GC': 0.28188279668266325, 'GCN4': [], '# scaffolds': 183, 'Completeness': 18.495297805642632, 'GCN3': [], 'GCN5+': [], '# contigs': 183, 'Translation table': 11, '# markers': 104, 'Coding density': 0.9122114587496243, 'Mean contig length': 3308.9945355191257, '# marker sets': 58, 'N50 (contigs)': 3222, '1': 19, '0': 85, '3': 0, '2': 0, 'Longest scaffold': 7264, '4': 0, '5+': 0, 'Contamination': 0.0, '# predicted genes': 621, 'N50 (scaffolds)': 3222, '# ambiguous bases': 0, 'Mean scaffold length': 3308.9945355191257},
            'bin.009': {'GC std': 0.019068777402063024, '# genomes': 5449, 'Genome size': 373747, 'Longest contig': 7332, 'GCN0': ['PF01509', 'PF00162', 'PF00889', 'TIGR00615', 'PF03484', 'PF04983', 'PF00562', 'PF00338', 'PF04563', 'PF01196', 'PF00281', 'PF00673', 'PF04997', 'PF00380', 'PF01245', 'PF10385', 'PF00411', 'TIGR00344', 'PF00416', 'TIGR00459', 'TIGR00755', 'TIGR00392', 'PF04565', 'PF02367', 'PF01668', 'PF00181', 'PF00312', 'PF13603', 'PF03948', 'PF00347', 'PF05000', 'PF00297', 'TIGR02432', 'TIGR00460', 'PF00177', 'PF00828', 'PF00886', 'TIGR03263', 'PF00333', 'PF00410', 'PF12344', 'PF00164', 'TIGR00967', 'PF01193', 'PF01765', 'PF01409', 'TIGR03594', 'PF08529', 'PF02130', 'PF03947', 'PF04560', 'TIGR00922', 'TIGR02075', 'PF01000', 'TIGR00810', 'PF01632', 'TIGR03723', 'PF00203', 'PF01121', 'PF01746', 'PF01195', 'PF00572', 'PF03719', 'PF00861', 'PF00466', 'PF04998', 'PF06071', 'TIGR00855', 'TIGR00329', 'TIGR00084', 'TIGR00250', 'PF00276', 'PF13184', 'PF04561', 'PF00687', 'PF01649', 'PF06421', 'PF05697', 'PF00453', 'PF00318', 'PF02978', 'PF02912', 'PF00623', 'PF02033', 'PF00573'], 'GCN1': ['PF00298', 'PF01018', 'PF01016', 'PF00189', 'PF01795', 'PF01281', 'PF00831', 'PF00252', 'PF11987', 'PF00366', 'PF03946', 'TIGR01079', 'PF00238', 'PF00829', 'PF00237', 'TIGR00019', 'PF01250', 'PF08459', 'PF05491'], 'GCN2': [], 'marker lineage': 'k__Bacteria', 'GC': 0.6536159487567793, 'GCN4': [], '# scaffolds': 107, 'Completeness': 16.901776384535005, 'GCN3': [], 'GCN5+': [], '# contigs': 107, 'Translation table': 11, '# markers': 104, 'Coding density': 0.8976179073009282, 'Mean contig length': 3492.96261682243, '# marker sets': 58, 'N50 (contigs)': 3305, '1': 19, '0': 85, '3': 0, '2': 0, 'Longest scaffold': 7332, '4': 0, '5+': 0, 'Contamination': 0.0, '# predicted genes': 444, 'N50 (scaffolds)': 3305, '# ambiguous bases': 0, 'Mean scaffold length': 3492.96261682243},
            'bin.006': {'GC std': 0.0180666565004557, '# genomes': 5449, 'Genome size': 520659, 'Longest contig': 25878, 'GCN0': ['PF00162', 'TIGR00615', 'PF00416', 'TIGR00459', 'TIGR00392', 'PF01668', 'TIGR02432', 'TIGR00460', 'PF00886', 'TIGR03263', 'PF01409', 'TIGR03594', 'PF08529', 'TIGR02075', 'TIGR03723', 'PF01121', 'PF01746', 'PF01195', 'PF00466', 'TIGR00855', 'TIGR00329', 'PF13184', 'PF00687', 'PF02978', 'PF02912'], 'GCN1': ['PF01509', 'PF00298', 'PF01018', 'PF00889', 'PF03484', 'PF04983', 'PF01016', 'PF00562', 'PF00338', 'PF04563', 'PF01196', 'PF00281', 'PF00673', 'PF00380', 'PF01245', 'PF10385', 'PF00411', 'PF00189', 'TIGR00344', 'PF01795', 'PF01281', 'PF00831', 'PF00252', 'TIGR00755', 'PF04565', 'PF02367', 'PF00181', 'PF11987', 'PF00312', 'PF13603', 'PF03948', 'PF00347', 'PF05000', 'PF00297', 'PF00366', 'PF00177', 'PF00828', 'PF03946', 'PF00333', 'PF00410', 'PF12344', 'PF00164', 'TIGR00967', 'PF01193', 'PF01765', 'PF02130', 'PF03947', 'PF04560', 'TIGR00922', 'PF01000', 'TIGR00810', 'TIGR01079', 'PF01632', 'PF00238', 'PF00203', 'PF00572', 'PF03719', 'PF00829', 'PF00861', 'PF00237', 'PF04998', 'PF06071', 'TIGR00084', 'TIGR00250', 'PF00276', 'PF04561', 'TIGR00019', 'PF01649', 'PF06421', 'PF05697', 'PF00453', 'PF01250', 'PF08459', 'PF00318', 'PF05491', 'PF00623', 'PF02033', 'PF00573'], 'GCN2': ['PF04997'], 'marker lineage': 'k__Bacteria', 'GC': 0.3391874528242093, 'GCN4': [], '# scaffolds': 75, 'Completeness': 66.32183908045978, 'GCN3': [], 'GCN5+': [], '# contigs': 75, 'Translation table': 11, '# markers': 104, 'Coding density': 0.8351224121738028, 'Mean contig length': 6942.12, '# marker sets': 58, 'N50 (contigs)': 7985, '1': 78, '0': 25, '3': 0, '2': 1, 'Longest scaffold': 25878, '4': 0, '5+': 0, 'Contamination': 0.15673981191222572, '# predicted genes': 614, 'N50 (scaffolds)': 7985, '# ambiguous bases': 0, 'Mean scaffold length': 6942.12},
        }


if __name__ == '__main__':
    unittest.main(verbosity=2)
