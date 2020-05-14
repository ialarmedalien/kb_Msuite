import unittest
import os
import json
import shutil
import sys
import logging

from TestEngine import TestEngine

from kb_Msuite.kb_MsuiteImpl import kb_Msuite
from kb_Msuite.Utils.CheckMUtil import CheckMUtil
from kb_Msuite.Utils.Utils import LogMixin


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
        # }, {
        #     'path': 'GCF_000022285.1_ASM2228v1_genomic.gbff',
        #     'name': 'Genome.C.3_4MB',
        #     'attr': 'genome_c_ref',
        # }, {
        #     'path': 'GCF_001439985.1_wTPRE_1.0_genomic.gbff',
        #     'name': 'Genome.D.2_5MB',
        #     'attr': 'genome_d_ref',
        },
    ],
    'genomeset_list': [],
    'binned_contigs_list': [
        {
            'path': 'binned_contigs',
            'name': 'Binned_Contigs',
            'attr': 'binned_contigs_ref',
            'assembly': 'assembly_OK_ref',
        }, {
            'path': 'binned_contigs_empty',
            'name': 'Binned_Contigs_Empty',
            'attr': 'binned_contigs_empty_ref',
            'assembly': 'assembly_OK_ref',
        }, {
            'path': 'binned_contigs_mini',
            'name': 'Mini_Binned_Contigs',
            'attr': 'binned_contigs_mini_ref',
            'assembly': 'assembly_mini_ref',
        },
    ],
    'report_list': [
        {
            'attr': 'report_ref',
            'name': 'The_Best_Report_In_The_Known_World',
            'data': {
                'report_object_name': 'The_Best_Report_In_The_Known_World',
                'message': 'This is the best report in the world',
            }
        }
    ]
}


class CheckMTestBase(LogMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(CheckMTestBase, cls).setUpClass()
        te = TestEngine.get_instance()
        if not te.env_set_up:
            te.set_up_test_env()

        cls.te = te
        # copy over the attributes
        attrs = [
            'cfg', 'ctx',
            'wsClient', 'wsName', 'ws_info',
            'au', 'gfu', 'mu', 'setAPI', 'kr',
            'refdata_wsName', 'refdata_ws_info',
            'appdir', 'scratch', 'test_data_dir', 'suffix',
            'data_loaded', 'github_run'
        ]

        for attr in attrs:
            setattr(cls, attr, getattr(te, attr))

        cls.serviceImpl = kb_Msuite(cls.cfg)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace ' + cls.wsName + ' was deleted')
        pass


class CheckReportMixin(unittest.TestCase):

    def run_and_check_report(self, params, expected=None, with_filters=False):
        '''
        Run 'run_checkM_lineage_wf' with or without filters, and check the resultant KBaseReport
        using check_report()

        Args:

          params        - dictionary of input params
          expected      - dictionary representing the expected structure of the KBaseReport object
          with_filters  - whether or not to use the 'withFilter' version of the workflow

        '''
        if (with_filters):
            result = self.getImpl().run_checkM_lineage_wf_withFilter(self.getContext(), params)[0]
        else:
            result = self.getImpl().run_checkM_lineage_wf(self.getContext(), params)[0]

        return self.check_report(result, expected)

    def check_report(self, result, expected):
        '''
        Test utility to check a KBaseReport object
        Args:

          result    - result returned by running KBaseReport.get_extended_report
                      { 'report_name': blahblahblah, 'report_ref': reference }

          expected  - dictionary representing the expected structure of the report
                      any keys omitted from the dictionary are assumed to be the report default
                      (None or an empty list)
        '''

        self.assertIn('report_name', result)
        self.assertIn('report_ref', result)

        # make sure the report was created and includes the HTML report and download links
        got_object = self.wsClient().get_objects2({
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
                elif key == 'objects_created' and expected['objects_created']:
                    # check input type -- if it is a dict, it will be of the form
                    # 'objects_created': [{
                    #   'description': 'HQ BinnedContigs filter.BinnedContigs',
                    #   'ref': '50054/17/1'
                    # }]
                    self.assertTrue(len(rep['objects_created']) == 1)
                    obj = rep['objects_created'][0]
                    self.assertTrue(len(obj.keys()) == 2)

                    eoc = expected['objects_created'][0]
                    self.assertEqual(
                        obj['description'],
                        eoc['description']
                    )
                    if eoc['ref'] == '__REF_UNKNOWN__':
                        self.assertRegex(obj['ref'], eoc['ref'])
                    else:
                        self.assertRegex(obj['ref'], r'\d+/\d+/\d+')
                else:
                    self.assertEqual(rep[key], report_data[key])

        return True

    def check_report_links(self, report_obj, type, expected):
        """
        Test utility: check the file upload results for an extended report
        Args:
          report_obj    - result dictionary from running KBaseReport.create_extended_report
          type          - one of "html_links" or "file_links"
          file_names    - names of the files for us to check against
        """
        file_links = report_obj[type]
        self.assertEqual(len(file_links), len(expected[type]))
        # Test that all the filenames listed in the report object map correctly
        saved_names = set([str(f['name']) for f in file_links])
        self.assertEqual(saved_names, set(expected[type]))
        return True


class CoreCheckMTestClient(CheckMTestBase):

    def getWsClient(self):
        return self.__class__.wsClient

    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    def getConfig(self):
        return self.__class__.serviceImpl.config

    def prep_checkMUtil(self):

        cmu = CheckMUtil(self.getConfig(), self.getContext())
        cmu.run_config()
        return cmu

    def clean_up_cmu(self, cmu):
        shutil.rmtree(cmu.run_config()['base_dir'], ignore_errors=True)

    def setUp(self):
        self.checkMUtil = self.prep_checkMUtil()

    def tearDown(self):
        self.clean_up_cmu(self.checkMUtil)
        delattr(self, 'checkMUtil')

    def get_data(self):
        return TEST_DATA

    def require_data(self, *args):
        if self.data_loaded:
            return True

        return self.prep_ref_data(args)

    def prep_ref_data(self, *args):

        try:
            # self.data_loaded = self.load_saved_ref_data()
            if not self.data_loaded:
                self.logger.info('Loading up ref data')
                self.prep_assemblies()
                self.prep_binned_contigs()
                self.prep_genomes()
                self.prep_report()
                self.data_loaded = True
        except Exception as e:
            self.logger.error('Error loading ref data!')
            self.logger.error(e)
            # exit the tests early

        if not self.data_loaded:
            sys.exit('Ref data could not be prepared. Dying.')

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

        # attempt to fetch a ref from the workspace
        try:
            self.wsClient.get_object_info3({'objects': [{
                'ref': saved_refs['report_ref']
            }]})['infos'][0]
        except Exception as e:
            self.logger.error({'ws_fetch_error': e})
            return False

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
        if hasattr(self, assembly['attr']):
            self.logger.info({
                assembly['attr']: getattr(self, assembly['attr']),
            })
            return

        assembly_file_path = os.path.join(self.test_data_dir, "assemblies", assembly['path'])
        if not os.path.exists(assembly_file_path):
            shutil.copy(os.path.join("data", "assemblies", assembly['path']), assembly_file_path)

        saved_assembly = self.au.save_assembly_from_fasta({
            'file': {
                'path': assembly_file_path
            },
            'workspace_name':   self.refdata_ws_info[1],
            'assembly_name':    assembly['name'],
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
        if hasattr(self, assemblyset['attr']):
            self.logger.info({
                assemblyset['attr']: getattr(self, assemblyset['attr']),
            })
            return

        saved_assembly_set = self.setAPI.save_assembly_set_v1({
            'workspace_name':     self.refdata_ws_info[1],
            'output_object_name': assemblyset['name'],
            'data': {
                'description': 'test assembly set',
                'items':        assemblyset['items'],
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
        if hasattr(self, bc['attr']):
            self.logger.info({
                bc['attr']: getattr(self, bc['attr']),
            })
            return

        binned_contigs_path = os.path.join(self.test_data_dir, bc['path'])
        if not os.path.exists(binned_contigs_path) or not os.path.exists(
            os.path.join(binned_contigs_path, 'bin.summary')
        ):
            shutil.rmtree(binned_contigs_path, ignore_errors=True)
            shutil.copytree(os.path.join("data", bc['path']), binned_contigs_path)

        saved_object = self.mu.file_to_binned_contigs({
            'file_directory':     binned_contigs_path,
            'workspace_name':     self.refdata_ws_info[1],
            'assembly_ref':       getattr(self, bc['assembly']),
            'binned_contig_name': bc['name'],
        })

        setattr(self, bc['attr'], saved_object['binned_contig_obj_ref'])
        self.logger.info({
            'Saved BinnedContigs': saved_object,
            bc['attr']:            getattr(self, bc['attr'])
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
        if hasattr(self, genome['attr']):
            self.logger.info({
                genome['attr']: getattr(self, genome['attr']),
            })
            return

        genome_file_path = os.path.join(self.test_data_dir, genome['path'])
        if not os.path.exists(genome_file_path):
            shutil.copy(os.path.join("data", "genomes", genome['path']), genome_file_path)

        genome_data = self.gfu.genbank_to_genome({
            'file': {
                'path': genome_file_path
            },
            'workspace_name':         self.refdata_ws_info[1],
            'genome_name':            genome['name'],
            'generate_ids_if_needed': 1,
        })
        setattr(self, genome['attr'], genome_data['genome_ref'])
        self.logger.info({
            'Saved Genome': genome_data,
            genome['attr']: getattr(self, genome['attr']),
        })

    def _prep_genomeset(self, genomeset):
        if hasattr(self, genomeset['attr']):
            self.logger.info({
                genomeset['attr']: getattr(self, genomeset['attr']),
            })
            return

        [OBJID_I, NAME_I, TYPE_I, SAVE_DATE_I, VERSION_I, SAVED_BY_I, WSID_I,
         WORKSPACE_I, CHSUM_I, SIZE_I, META_I] = list(range(11))  # object_info tuple

        obj_info = self.wsClient.save_objects({
            'workspace': self.refdata_ws_info[1],
            'objects': [{
                'type': 'KBaseSearch.GenomeSet',
                'data': {
                    'description': 'genomeSet for testing',
                    'elements': genomeset['items'],
                },
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
        for genome in genome_list:
            self._prep_genome(genome)

        genomeset_list = [
            {
                # create a genomeSet from the first three genomes
                'name': 'Small_GenomeSet',
                'attr': 'genome_set_small_ref',
                'items': {
                    genome['name']: {
                        'ref': getattr(self, genome['attr'])
                    } for genome in genome_list[0:3]
                },
            },
        ]

        for genomeset in genomeset_list:
            self._prep_genomeset(genomeset)

        return True

    def _prep_report(self, report):
        if hasattr(self, report['attr']):
            self.logger.info({
                report['attr']: getattr(self, report['attr']),
            })
            return

        report['data']['workspace_name'] = self.refdata_ws_info[1]
        # create the kbasereport object
        report_output = self.kr.create_extended_report(report['data'])

        setattr(self, report['attr'], report_output['ref'])
        self.logger.info({
            'Saved Report': report_output,
            report['attr']: getattr(self, report['attr']),
        })

    def prep_report(self):
        ''' copy templates into the test data directory and create a KBaseReport ref '''

        for report in TEST_DATA['report_list']:
            self._prep_report(report)

        return

        # copy over the templates
        test_tmpl_dir = os.path.join(self.test_data_dir, 'templates')
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


def print_method_name(method):
    def wrapper(*args, **kwargs):
        method_name = method.__name__
        method_name.replace("test_", "")
        logger = logging.getLogger('kb_Msuite.CoreCheckMTest')
        logger.info("=================================================================")
        logger.info("RUNNING " + method_name)
        logger.info("=================================================================\n")
        return method(*args, **kwargs)
    return wrapper
