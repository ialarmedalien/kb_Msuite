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
from kb_Msuite.Utils.Utils import LogMixin, TSVMixin

from kb_Msuite.Utils.FileUtils import (
    clean_up_bin_ID,
    fasta_seq_len_at_least,
    get_fasta_files,
    set_fasta_file_extensions,
    read_bin_stats_file
)


def print_method_name(method):
    def wrapper(*args, **kwargs):
        method_name = method.__name__
        method_name.replace("test_", "")
        self.logger.info("=================================================================")
        self.logger.info("RUNNING " + method_name)
        self.logger.info("=================================================================\n")
        return method(*args, **kwargs)
    return wrapper


TEST_DATA = {
    'assembly_list': [
        {
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
        }, {
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
        },
    ],
    'assemblyset_list': [],
    'genome_list': [
        {
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
            'path': 'GCF_000022285.1_ASM2228v1_genomic.gbff',
            'name': 'Genome.C.3_4MB',
            'attr': 'genome_c_ref',
        }, {
            'path': 'GCF_001439985.1_wTPRE_1.0_genomic.gbff',
            'name': 'Genome.D.2_5MB',
            'attr': 'genome_d_ref',
        },
    ],
    'genomeset_list': [],
    'binned_contigs_list': [
        {
            'path': 'binned_contigs',
            'name': 'Binned_Contigs',
            'attr': 'binned_contigs_ref',
        }, {
            'path': 'binned_contigs_empty',
            'name': 'Binned_Contigs_Empty',
            'attr': 'binned_contigs_empty_ref',
        }, {
            'path': 'binned_contigs_mini',
            'name': 'Mini_Binned_Contigs',
            'attr': 'binned_contigs_mini_ref',
        },
    ],
}


class CoreCheckMTest(unittest.TestCase, LogMixin, TSVMixin):

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
        cls.refdata_ws_info = [
            49697,
            'test_kb_Msuite_refdata_1588183380977',
            'ialarmedalien',
            '2020-04-29T18:03:01+0000',
            0, 'a', 'n', 'unlocked', {}
        ]

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

    def getWsClient(self):
        return self.__class__.wsClient

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def getConfig(self):
        return self.__class__.serviceImpl.config

    def require_data(self, *args):
        if self.data_loaded:
            return True

        return self.prep_ref_data()

    def prep_ref_data(self):

        try:
            self.load_saved_ref_data()
            # self.prep_binned_contigs()
            # self.prep_genomes()
            # self.prep_assemblies()
            # self.prep_report()
            self.data_loaded = True
        except Exception as e:
            self.logger.error('Error loading ref data!')
            self.logger.error(e)
            import sys
            sys.exit('Ref data could not be prepared. Dying.')
            # exit the tests early

        return True

    def load_saved_ref_data(self):
        saved_refs = {
            'assembly_OK_ref': '49697/1/1',
            'assembly_dodgy_ref': '49697/2/1',
            'assembly_a_ref': '49697/15/1',
            'assembly_b_ref': '49697/16/1',
            'assembly_virus_ref': '49697/14/1',
            'assembly_mini_ref': '49697/12/1',
            # assembly set
            'assembly_set_ref': '49697/3/1',
            'assembly_set_smaller_ref': '49697/24/1',
            'assembly_set_small_ref': '49697/24/2',
            # binned contigs
            'binned_contigs_ref': '49697/4/1',
            'binned_contigs_empty_ref': '49697/5/1',
            'binned_contigs_mini_ref': '49697/13/1',
            # genomes
            'genome_virus_ref': '49697/18/1',
            'genome_a_ref': '49697/20/1',
            'genome_b_ref': '49697/22/1',
            'genome_c_ref': '49697/8/1',
            'genome_d_ref': '49697/10/1',
            # genome set
            'genome_set_ref': '49697/11/1',
            'genome_set_smaller_ref': '49697/26/7',
            'genome_set_small_ref': '49697/26/17',
            # KBaseReport
            'report_ref': '49697/6/1',
        }

        for key, value in saved_refs.items():
            setattr(self, key, value)

            self.assertEqual(getattr(self, key), saved_refs[key])

        self.data_loaded = True
        return True

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
            'Saved Assembly': saved_assembly,
            assembly['attr']: getattr(self, assembly['attr']),
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
        self.logger.info({
            'Saved AssemblySet': saved_assembly_set,
            assemblyset['attr']: getattr(self, assemblyset['attr']),
        })
        TEST_DATA['assemblyset_list'].append(assemblyset)

    def prep_assemblies(self):
        ''' prepare the assemblies and assembly set '''

        assembly_list = TEST_DATA['assembly_list']

        for assembly in assembly_list:
            self._prep_assembly(assembly)

        assemblyset_list = [
            {
                'name': 'Small_Assembly_Set',
                'attr': 'assembly_set_small_ref',
                'items': [
                    {
                        'ref':   getattr(self, a['attr']),
                        'label': a['name'],
                    }
                    for a in assembly_list[0:3]
                ],
            },
        ]

        for assemblyset in assemblyset_list:
            self._prep_assemblyset(assemblyset)

        return True

    def _prep_binned_contig(self, bc):

        binned_contigs_path = os.path.join(self.test_data_dir, bc['path'])
        if not os.path.exists(binned_contigs_path) or not os.path.exists(
            os.path.join(binned_contigs_path, 'bin.summary')
        ):
            shutil.rmtree(binned_contigs_path, ignore_errors=True)
            shutil.copytree(os.path.join("data", bc['path']), binned_contigs_path)

        saved_object = self.mu.file_to_binned_contigs({
            'file_directory': binned_contigs_path,
            'workspace_name': self.refdata_ws_info[1],
            'assembly_ref': self.assembly_OK_ref,
            'binned_contig_name': bc['name'],
        })

        setattr(self, bc['attr'], saved_object['binned_contig_obj_ref'])
        self.logger.info({
            'Saved BinnedContigs': saved_object,
            bc['attr']: getattr(self, bc['attr'])
        })

    def prep_binned_contigs(self):

        # make sure we have assemblies loaded
        for assembly in TEST_DATA['assembly_list']:
            if not hasattr(self, assembly['attr']):
                self.prep_assemblies()
                break

        # some binned contigs
        binned_contigs_list = TEST_DATA['binned_contigs_list']
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
            genome['attr']: getattr(self, genome['attr']),
        })

    def _prep_genomeset(self, genomeset):
        [OBJID_I, NAME_I, TYPE_I, SAVE_DATE_I, VERSION_I, SAVED_BY_I, WSID_I,
         WORKSPACE_I, CHSUM_I, SIZE_I, META_I] = list(range(11))  # object_info tuple

        obj_info = self.wsClient.save_objects({
            'workspace': self.refdata_ws_info[1],
            'objects': [{
                'type': 'KBaseSearch.GenomeSet',
                'data': genomeset['data'],
                'name': genomeset['name'],
                'meta': {},
                'provenance': [{
                    'service': 'kb_Msuite',
                    'method':  'test_CheckM'
                }]
            }]
        })[0]
        reference = "/".join([str(obj_info[prop]) for prop in [WSID_I, OBJID_I, VERSION_I]])

        setattr(self, genomeset['attr'], reference)
        self.logger.info({
            'Saved Genomeset': obj_info,
            genomeset['attr']: getattr(self, genomeset['attr'])
        })

        TEST_DATA['genomeset_list'].append(genomeset)

    def prep_genomes(self):

        ''' add a couple of genomes and create a genome set '''

        genome_list = TEST_DATA['genome_list']

        # upload a few genomes
        # for genome in genome_list:
        #     self._prep_genome(genome)

        genomeset_list = [
            {
                # create a genomeSet from the first three genomes
                'name': 'Small_GenomeSet',
                'attr': 'genome_set_small_ref',
                'data': {
                    'description': 'genomeSet for testing',
                    'elements': {
                        genome['name']: {
                            'ref': getattr(self, genome['attr'])
                        } for genome in genome_list[0:3]
                    },
                },
            },
        ]

        for genomeset in genomeset_list:
            self._prep_genomeset(genomeset)

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

    def prep_checkMUtil(self):

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.run_config()
        return cmu

    def clean_up_cmu(self, cmu):

        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

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

        self.clean_up_cmu(cmu)

    def test_01_fileutils_clean_up_bin_id(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING 01_fileutils_clean_up_bin_id")
        self.logger.info("=================================================================\n")

        expected = [
            ['bin.056.fasta', 'bin.056', 'bin.056.fasta'],
            ['out_header_05.fasta', 'out_header_05', 'out_header_05.fasta'],
            ['assembly.fasta', 'assembly', 'assembly.fasta'],
            ['../../this.is.fake', '../../this.is.fake', '../../this.is'],
        ]

        for bid in expected:
            clean_ID = clean_up_bin_ID(bid[0], 'fasta')
            self.assertEqual(clean_ID, bid[1])
            clean_ID = clean_up_bin_ID(bid[0], '.fasta')
            self.assertEqual(clean_ID, bid[1])
            clean_ID = clean_up_bin_ID(bid[0], '.fake')
            self.assertEqual(clean_ID, bid[2])

    def test_00_workspace_helper(self):

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

            obj_type = cmu.workspacehelper.get_ws_obj_type(
                report_output['ref']
            )
            self.assertEqual(obj_type, 'KBaseReport.Report')

            obj_type = cmu.workspacehelper.get_ws_obj_type(
                object_info=ws_obj_info, remove_module=False
            )
            self.assertEqual(obj_type, 'KBaseReport.Report')

            obj_type = cmu.workspacehelper.get_ws_obj_type(
                ref=report_output['ref'], remove_module=True
            )
            self.assertEqual(obj_type, 'Report')

            obj_type = cmu.workspacehelper.get_ws_obj_type(
                object_info=ws_obj_info, remove_module=True
            )
            self.assertEqual(obj_type, 'Report')

        with self.subTest('get obj from workspace'):
            ws_obj = cmu.workspacehelper.get_obj_from_workspace(report_output['ref'])
            self.logger.info(ws_obj)
            self.assertEqual(ws_obj['text_message'], text_message)

            err_str = 'Unable to fetch ROTFLMAO object from workspace:'
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.workspacehelper.get_obj_from_workspace('ROTFLMAO')

        self.clean_up_cmu(cmu)

    def test_00_init_client(self):

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

    def test_01_fileutils_fasta_seq_len_at_least(self):

        assembly_dir = os.path.join('data', 'assemblies')
        empty_assembly_path = os.path.join(assembly_dir, 'empty_assembly.fasta')
        one_nt_assembly_path = os.path.join(assembly_dir, '1_nt_assembly.fasta')
        big_assembly_path = os.path.join(assembly_dir, 'assembly.fasta')

        # default: min length 1
        self.assertFalse(
            fasta_seq_len_at_least(empty_assembly_path)
        )
        self.assertTrue(
            fasta_seq_len_at_least(one_nt_assembly_path)
        )
        self.assertTrue(
            fasta_seq_len_at_least(big_assembly_path)
        )
        # set the min length to 2
        self.assertFalse(
            fasta_seq_len_at_least(empty_assembly_path, 2)
        )
        self.assertFalse(
            fasta_seq_len_at_least(one_nt_assembly_path, 2)
        )
        self.assertTrue(
            fasta_seq_len_at_least(big_assembly_path, 2)
        )

        with self.assertRaisesRegexp(ValueError, 'Minimum length must be 1 or greater'):
            fasta_seq_len_at_least(empty_assembly_path, 0)

    def test_01_fileutils_set_fasta_file_extensions(self):

        cmu = self.prep_checkMUtil()
        run_config = cmu.run_config()
        test_dir = os.path.join(run_config['base_dir'], 'test_dir')
        os.makedirs(test_dir, exist_ok=True)

        extensions = ['.fasta', '.fas', '.fa', '.fsa', '.seq', '.fna', '.ffn', '.faa', '.frn']
        invalid_ext = ['.jpg', '.fasta.tar.gz', '.fa.zip', '.fnaaaar']

        dir_inventory = {}
        n = 0
        for ext in extensions + invalid_ext:
            file_path = os.path.join(test_dir, 'file_00' + str(n) + ext)
            Path(file_path).touch()
            self.assertTrue(os.path.isfile(file_path))
            dir_inventory['file_00' + str(n)] = file_path
            n += 1

        fasta_files_by_clean_bin_ID = get_fasta_files(test_dir, 'fasta')
        self.assertEqual({'file_000': dir_inventory['file_000']}, fasta_files_by_clean_bin_ID)

        # no files with the new extension (yet)
        self.assertEqual({}, get_fasta_files(test_dir, '007'))

        set_fasta_file_extensions(test_dir, '007')
        # expect 1 - 9 to have the extension .007
        new_dir_inventory = {}
        n = 0
        for ext in extensions:
            ext_file = os.path.join(test_dir, 'file_00' + str(n) + '.007')
            self.assertTrue(os.path.isfile(ext_file))
            new_dir_inventory['file_00' + str(n)] = ext_file
            n += 1

        for ext in invalid_ext:
            pos = n - len(extensions)
            self.assertTrue(os.path.isfile(
                os.path.join(test_dir, 'file_00' + str(n) + invalid_ext[pos]))
            )
            n += 1

        # ensure that we get the expected results from get_fasta_files
        fasta_files_by_clean_bin_ID = get_fasta_files(test_dir, 'fasta')
        self.assertEqual({}, get_fasta_files(test_dir, 'fasta'))
        self.assertEqual(new_dir_inventory, get_fasta_files(test_dir, '007'))

        # check that set_fasta_file_extensions will happily overwrite an extant file
        # TODO: it would be better if this complained, rather than automatically
        # overwriting the file
        new_file = os.path.join(test_dir, 'file_007.faa')
        Path(os.path.join(test_dir, 'file_007.faa')).touch()
        self.assertTrue(os.path.isfile(new_file))
        self.assertTrue(os.path.isfile(os.path.join(test_dir, 'file_007.007')))
        set_fasta_file_extensions(test_dir, '.007')
        self.assertFalse(os.path.exists(new_file))
        self.assertTrue(os.path.isfile(os.path.join(test_dir, 'file_007.007')))

        self.clean_up_cmu(cmu)

    def test_01_fileutils_read_bin_stats_file(self):

        # non-existent file: return empty dict
        self.assertEqual({}, read_bin_stats_file('/path/to/pretend/file'))

        # this could do with more testing...

    def notest_01_fileutils_cat_fasta_files(self):
        # folder, extension, output_fasta_file):
        '''
        Given a folder of fasta files with the specified extension, cat them together
        using 'cat' into the target new_fasta_file
        '''
        # files = glob.glob(os.path.join(folder, '*.' + extension))
        # cat_cmd = ['cat'] + files
        # fasta_fh = open(output_fasta_file, 'w')
        # p = subprocess.Popen(cat_cmd, cwd=self.scratch, stdout=fasta_fh, shell=False)
        # exitCode = p.wait()
        # fasta_fh.close()

        # if exitCode != 0:
        #     raise ValueError('Error running command: ' + ' '.join(cat_cmd) + '\n' +
        #                      'Exit Code: ' + str(exitCode))

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

        cmu = self.prep_checkMUtil()

        with self.subTest('erroneous report object staging'):
            err_msg = 'Cannot stage fasta file input directory from type: '
            with self.assertRaisesRegex(ValueError, err_msg):
                cmu.datastagingutils.stage_input(self.report_ref)

        # empty input files/objects:
        # cannot create an empty assembly or an empty genome object
        # can create a binnedcontigs obj with an empty assembly
        with self.subTest('One empty assembly in BinnedContigs'):
            err_str = 'Binned Assembly is empty for fasta_path: '
            with self.assertRaisesRegex(ValueError, err_str):
                cmu.datastagingutils.stage_input(self.binned_contigs_empty_ref)

        cmu.datastagingutils.stage_input('here_is_a_made_up_ref')

        self.clean_up_cmu(cmu)

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
        cmu = self.prep_checkMUtil()

        staged_input = cmu.datastagingutils.stage_input(self.assembly_mini_ref)
        self.assertEqual(
            staged_input,
            {'obj_name': 'MiniAssembly', 'obj_type': 'KBaseGenomeAnnotations.Assembly'}
        )
        self.check_data_staging_results(cmu.run_config(), ['MiniAssembly'])

        self.clean_up_cmu(cmu)

    def test_02_data_staging_assembly_strange_fasta_ext(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_assembly_strange_fasta_ext")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_OK_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.fasta_extension = 'strange_fasta_extension'
        run_config = cmu._set_run_config()
        staged_input = cmu.datastagingutils.stage_input(self.assembly_OK_ref)
        self.assertEqual(
            staged_input,
            {'obj_name': 'Test.Assembly', 'obj_type': 'KBaseGenomeAnnotations.Assembly'}
        )
        self.check_data_staging_results(run_config, ['Test.Assembly'])

        self.clean_up_cmu(cmu)

    def test_02_data_staging_assemblyset(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_assemblyset")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_set_ref')
        cmu = self.prep_checkMUtil()
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

        self.clean_up_cmu(cmu)

    def test_02_data_staging_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
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

        self.clean_up_cmu(cmu)

    def test_02_data_staging_genome(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_genome")
        self.logger.info("=================================================================\n")

        self.require_data('genome_refs')

        test_genome = TEST_DATA['genome_list'][0]

        cmu = self.prep_checkMUtil()
        staged_input = cmu.datastagingutils.stage_input(getattr(self, test_genome['attr']))
        self.assertEqual(
            staged_input,
            {
                'obj_name': test_genome['name'],
                'obj_type': 'KBaseGenomes.Genome',
            }
        )
        self.check_data_staging_results(cmu.run_config(), [test_genome['name']])

        self.clean_up_cmu(cmu)

    def test_02_data_staging_genome_set(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 02_data_staging_genome_set")
        self.logger.info("=================================================================\n")

        self.require_data('genome_set_small_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu._set_run_config()

        genome_list = TEST_DATA['genome_list'][0:3]

        staged_input = cmu.datastagingutils.stage_input(self.genome_set_small_ref)
        self.assertEqual(
            staged_input,
            {
                'obj_name': 'Small_GenomeSet',
                'obj_type': 'KBaseSearch.GenomeSet',
            }
        )
        self.check_data_staging_results(run_config, [g['name'] for g in genome_list])

        self.clean_up_cmu(cmu)

    def test_03_filter_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref', 'report_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.run_config()

        # wrong type
        self.assertIsNone(
            cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.report_ref})
        )
        self.assertIsNone(
            cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.assembly_dodgy_ref})
        )

        # no output_filtered_binnedcontigs_obj_name
        self.assertIsNone(
            cmu.binnedcontigfilter.filter_binned_contigs({'input_ref': self.binned_contigs_ref})
        )

        # empty input dir
        os.makedirs(cmu.run_config()['input_dir'], exist_ok=True)
        self.assertIsNone(cmu.binnedcontigfilter.filter_binned_contigs({
            'input_ref': self.binned_contigs_ref,
            'output_filtered_binnedcontigs_obj_name': 'Alpha',
        }))

        self.clean_up_cmu(cmu)

    def prep_filter_binned_contigs_dirs(self):

        self.require_data('binned_contigs_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        cmu.fasta_extension = 'fasta'
        cmu.run_config()
        run_config = cmu.run_config()

        # copy over a results file
        # 001  'Completeness': 97.6,              'Contamination': 1.907,
        # 002  'Completeness': 98.11542991755006, 'Contamination': 1.4134275618374559,
        # 003  'Completeness': 96.34019795657727, 'Contamination': 1.7600574712643677,
        output_dir = run_config['output_dir']
        os.makedirs(os.path.join(output_dir, 'storage'), exist_ok=True)
        shutil.copy(
            os.path.join('data', 'filter_all_fail.bin_stats_ext.tsv'),
            run_config['bin_stats_ext_file']
        )

        os.makedirs(run_config['input_dir'], exist_ok=True)
        for bid in [1, 2, 3]:
            bid_path = os.path.join(
                run_config['input_dir'],
                'bin.00' + str(bid) + '.' + run_config['fasta_ext']
            )
            Path(bid_path).touch(exist_ok=True)

        return cmu

    def test_03_filter_binned_contigs_checkM_missing_IDs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_checkM_missing_IDs")
        self.logger.info("=================================================================\n")

        cmu = self.prep_filter_binned_contigs_dirs()
        run_config = cmu.run_config()

        missing_ids = ['bin.000', 'bin.004']
        for bid in missing_ids:
            bid_path = os.path.join(
                run_config['input_dir'], bid + '.' + run_config['fasta_ext']
            )
            Path(bid_path).touch(exist_ok=True)

        err_str = "The following bin IDs are missing from the checkM output: " + \
            ", ".join(missing_ids)
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.binnedcontigfilter.filter_binned_contigs({
                'input_ref': self.binned_contigs_ref,
                'output_filtered_binnedcontigs_obj_name': 'Beta',
            })

        self.clean_up_cmu(cmu)

    def test_03_filter_binned_contigs_no_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_no_HQ")
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

        self.clean_up_cmu(cmu)

    def test_03_filter_binned_contigs_all_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_all_HQ")
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

        self.clean_up_cmu(cmu)

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
            expected_path = os.path.join(
                run_config['filtered_bins_dir'], id + '.' + run_config['fasta_ext']
            )
            self.assertTrue(os.path.isfile(expected_path))
        #

    def test_03_filter_binned_contigs_some_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_some_HQ")
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
            'retained_bin_IDs': {'bin.002': True},
            'removed_bin_IDs':  {'bin.001': True, 'bin.003': True}
        }
        self.check_filtered_bins(cmu, run_config, results, expected)
        self.clean_up_cmu(cmu)

    def test_03_filter_binned_contigs_some_others_HQ(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 03_filter_binned_contigs_some_others_HQ")
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
            'retained_bin_IDs': {'bin.001': True, 'bin.002': True},
            'removed_bin_IDs':  {'bin.003': True},
        }
        self.check_filtered_bins(cmu, run_config, results, expected)
        self.clean_up_cmu(cmu)

    def mimic_tsv_output(self, cmu, tab_text_file):

        run_config = cmu.run_config()
        results_filtered = run_config['results_filtered']
        with open(tab_text_file, 'w', newline='') as tab_text_fh:
            tsv_writer = cmu.outputbuilder.init_tsv_writer(tab_text_fh)
            cmu.outputbuilder.write_tsv_headers(tsv_writer, results_filtered)

    def test_05_write_tsv_headers(self):

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu.run_config()
        os.makedirs(run_config['base_dir'], exist_ok=True)

        with self.subTest('No output filters in place'):
            filename = os.path.join(run_config['base_dir'], 'tab_text_no_filter.tsv')
            self.mimic_tsv_output(cmu, filename)
            # now read in the file and check the data is correct
            with open(filename, 'r') as tab_text_fh:
                lines = tab_text_fh.read().rstrip()
            self.assertRegex(lines, r'Has Plot File$')

        with self.subTest('Output filtered'):
            run_config['results_filtered'] = True
            filename = os.path.join(run_config['base_dir'], 'tab_text_with_filter.tsv')
            self.mimic_tsv_output(cmu, filename)
            with open(filename, 'r', newline='') as tab_text_fh:
                lines = tab_text_fh.read().rstrip()
            # the last col will be 'QA Pass'
            self.assertRegex(lines, r'QA Pass$')

    def test_05_outputbuilder_genome_assembly_set(self):

        self.require_data('assembly_set_small_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu.run_config()

        # prep the data
        os.makedirs(run_config['base_dir'], exist_ok=True)
        for dir in ['output', 'plots']:
            shutil.copytree(
                os.path.join('data', 'results', 'assemblyset', dir),
                os.path.join(run_config['base_dir'], dir)
            )

        assemblies = TEST_DATA['assembly_list'][0:3]

        bin_stats_data = read_bin_stats_file(run_config['bin_stats_ext_file'])
        params = {}
        html_files = cmu.outputbuilder.build_html_tsv_files(bin_stats_data, params)

        # html_files[0] is the summary template
        self.assertEqual(html_files[0]['name'], 'checkm_results.html')
        self.assertTrue('template' in html_files[0])

        # html_files[1] is the TSV file
        self.assertEqual(html_files[1], {
            'name': run_config['tab_text_file_name'],
            'path': run_config['tab_text_file'],
        })
        expected_content_file = os.path.join(
            'data', 'results', 'assemblyset', 'summary_table.tsv'
        )
        result_lines = open(html_files[1]['path'], 'r').read().splitlines(keepends=False)
        expected_lines = open(expected_content_file, 'r').read().splitlines(keepends=False)
        self.assertEqual(result_lines, expected_lines)

        # html_files[2] is the html plots dir
        self.assertEqual(html_files[2], {
            'name': 'plots',
            'path': os.path.join(run_config['html_dir'], 'plots')
        })
        # ensure that the plots dir contains the correct plots
        plot_file_ext = cmu.outputbuilder.PLOT_FILE_EXT
        for a in assemblies:
            plot_file = os.path.join(run_config['html_dir'], 'plots', a['name'] + plot_file_ext)
            self.assertTrue(os.path.isfile(plot_file))

        # 3-5 are the dist files
        self.assertEqual(
            set(a['name'] + '.html' for a in assemblies),
            set(h['name'] for h in html_files[3:]),
        )

        self.clean_up_cmu(cmu)

    def test_05_outputbuilder_binned_contigs(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 05_outputbuilder_binned_contigs")
        self.logger.info("=================================================================\n")

        self.require_data('binned_contigs_ref')

        cmu = CheckMUtil(self.cfg, self.ctx)
        run_config = cmu.run_config()

        # 'bin.010' has no plot file
        ids_with_plots = ['002', '005', '006', '009', '014', '033']

        dist_files = [run_config['bin_basename'] + '.' + id + '.html' for id in ids_with_plots]
        # lots of output:
        with self.subTest('binned contig, no filtering'):
            os.makedirs(run_config['base_dir'], exist_ok=True)
            for dir in ['output', 'plots']:
                shutil.copytree(
                    os.path.join('data', 'results', 'binned_contigs', dir),
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
                ] + dist_files,
            }
            self.check_report(result, expected_results)
            # check the TSV file: there should be no QA Pass col
            with open(run_config['tab_text_file'], newline='') as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                for row in reader:
                    self.logger.debug({'row': row})
                    # if the bin has a plot file, 'Has Plot File' will be true.
                    has_plot_file = 'True' if row['Bin Name'] + '.html' in dist_files else 'False'
                    self.assertEqual(row['Has Plot File'], has_plot_file)
                    self.assertNotIn('QA Pass', row)

        with self.subTest('binned contig, filters, new object created'):

            run_config['results_filtered'] = True
            for dir in ['html_dir', 'tab_text_dir', 'template_dest_dir']:
                shutil.rmtree(run_config[dir], ignore_errors=True)

            params = {'workspace_name': self.ws_info[1]}
            removed_bin_IDs = ['bin.002', 'bin.005', 'bin.033']
            filtered_obj_info = {
                'filtered_obj_ref': self.binned_contigs_ref,
                'filtered_obj_name': 'Nancy Drew',
                'removed_bin_IDs': {
                    bin_ID: True for bin_ID in removed_bin_IDs
                },
            }
            self.logger.debug({'filtered_obj_info': filtered_obj_info})

            result = cmu.outputbuilder.build_report(params, filtered_obj_info)

            self.assertEqual(
                set(result.keys()),
                set(['report_name', 'report_ref', 'binned_contig_obj_ref'])
            )
            self.assertEqual(result['binned_contig_obj_ref'], self.binned_contigs_ref)
            expected_results = {
                'direct_html_link_index': 0,
                # note: no plots dir
                'file_links': ['CheckM_summary_table.tsv', 'full_output'],
                'html_links': [
                    'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                ] + dist_files,
                'objects_created': [{
                    'ref': filtered_obj_info['filtered_obj_ref'],
                    'description':  'HQ BinnedContigs ' + filtered_obj_info['filtered_obj_name'],
                }]
            }

            self.check_report(result, expected_results)
            # there should be one bin with a missing plot file
            with open(run_config['tab_text_file'], newline='') as infile:
                reader = csv.DictReader(infile, delimiter='\t')
                for row in reader:
                    self.logger.debug({'row': row})
                    # if the bin has a plot file, 'Has Plot File' will be true.
                    has_plot_file = 'True' if row['Bin Name'] + '.html' in dist_files else 'False'
                    self.assertEqual(row['Has Plot File'], has_plot_file)
                    # if the bin is in 'removed_bin_IDs', it will have failed QA
                    qa_pass = 'False' if row['Bin Name'] in removed_bin_IDs else 'True'
                    self.assertEqual(row['QA Pass'], qa_pass)

        self.clean_up_cmu(cmu)

    def test_05_outputbuilder_no_checkM_output(self):

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
        self.clean_up_cmu(cmu)

    # Test 1: single assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_single_assembly")
    def test_checkM_end_to_end_single_assembly(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_single_assembly")
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
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots', 'MiniAssembly.html'
            ],
        }

        self.run_and_check_report(params, expected_results)

    # Test 2: Regression test (CheckM <= v1.0.7) for single problem assembly
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_single_problem_assembly")
    def notest_checkM_end_to_end_single_problem_assembly(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_single_problem_assembly")
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
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                'something.html'
            ],
        }

        self.run_and_check_report(params, expected_results)

    # Test 3: binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_binned_contigs")
    def notest_checkM_end_to_end_binned_contigs(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_binned_contigs")
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
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_binned_contigs_EMPTY")
    def notest_checkM_end_to_end_binned_contigs_EMPTY(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_binned_contigs_EMPTY")
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
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_assemblySet")
    def notest_checkM_end_to_end_assemblySet(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_assemblySet")
        self.logger.info("=================================================================\n")

        self.require_data('assembly_set_small_ref')
        # run checkM lineage_wf app on an assembly set
        input_ref = self.assembly_set_small_ref
        params = {
            'dir_name': 'assembly_set_small',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        html_files = [a['name'] + '.html' for a in TEST_DATA['assembly_list'][0:3]]
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
            ] + html_files,
        }

        self.run_and_check_report(params, expected_results)

    # Test 6: Single Genome
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_single_genome")
    def notest_checkM_end_to_end_single_genome(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_single_genome")
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
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
                'GCF_0000222851_ASM2228v1_genomicgbff.html'
            ],
        }
        # correct the file name!
        self.run_and_check_report(params, expected_results)

    # Test 7: Genome Set
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_genomeSet")
    def notest_checkM_end_to_end_genomeSet(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING checkM_end_to_end_genomeSet")
        self.logger.info("=================================================================\n")

        self.require_data('genome_set_small_ref')
        # run checkM lineage_wf app on a genome set
        input_ref = self.genome_set_small_ref
        params = {
            'dir_name': 'genome_set_small',
            'workspace_name': self.ws_info[1],
            'input_ref': input_ref,
            'reduced_tree': 1,
            'save_output_dir': 1,
            'save_plots_dir': 1,
            'threads': 4
        }

        html_files = [a['name'] + '.html' for a in TEST_DATA['genome_list'][0:3]]
        expected_results = {
            'direct_html_link_index': 0,
            'file_links': ['CheckM_summary_table.tsv', 'plots', 'full_output'],
            'html_links': [
                'checkm_results.html', 'CheckM_summary_table.tsv', 'plots',
            ] + html_files,
        }

        self.run_and_check_report(params, expected_results)

    # Test 11: filter binned contigs to HQ binned contigs
    #
    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_checkM_end_to_end_filter_binned_contigs")
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
    def notest_04_checkM_local_function_wiring(self):

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
        scratch_output_dir = os.path.join(
            self.scratch, 'lineage_wf_output_dir' + '_' + str(self.suffix)
        )
        shutil.copytree(test_data_dir, scratch_input_dir)
        if not os.path.exists(scratch_output_dir):
            os.mkdir(scratch_output_dir)
        log_path = os.path.join(self.scratch, 'lineage_wf.log')
        return scratch_input_dir, scratch_output_dir, log_path

    # Uncomment to skip this test
    # HIDE @unittest.skip("skipped test_local_method()")
    def notest_04_local_method(self):
        self.logger.info("=================================================================")
        self.logger.info("RUNNING 04_local_method")
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
