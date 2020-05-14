import os
import shutil
import time
from os import environ
from configparser import ConfigParser

from installed_clients.AssemblyUtilClient import AssemblyUtil
from installed_clients.GenomeFileUtilClient import GenomeFileUtil
from installed_clients.KBaseReportClient import KBaseReport
from installed_clients.MetagenomeUtilsClient import MetagenomeUtils
from installed_clients.SetAPIServiceClient import SetAPI
from installed_clients.WorkspaceClient import Workspace

from kb_Msuite.kb_MsuiteServer import MethodContext
from kb_Msuite.authclient import KBaseAuth as _KBaseAuth
from kb_Msuite.Utils.Utils import LogMixin


class TestEngine(LogMixin):

    __instance = None

    @staticmethod
    def get_instance():
        if TestEngine.__instance is None:
            TestEngine()
            return TestEngine.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if TestEngine.__instance is not None:
            raise Exception("Please use 'get_instance' to init the test engine")
        else:
            self.env_set_up = False
            TestEngine.__instance = self

    def set_up_test_env(self):

        token = environ.get('KB_AUTH_TOKEN', None)
        config_file = environ.get('KB_DEPLOYMENT_CONFIG', None)
        test_time_stamp = int(time.time() * 1000)

        self.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('kb_Msuite'):
            self.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = self.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        self.ctx = MethodContext(None)
        self.ctx.update({
            'token': token,
            'user_id': user_id,
            'provenance': [{
                'service': 'kb_Msuite',
                'method': 'please_never_use_it_in_production',
                'method_params': []
            }],
            'authenticated': 1
        })
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.scratch = self.cfg['scratch']
        self.appdir = self.cfg['appdir']
        self.suffix = test_time_stamp

        self.wsURL = self.cfg['workspace-url']
        self.wsClient = Workspace(self.wsURL)
        self.wsName = "test_kb_Msuite_" + str(self.suffix)
        self.ws_info = self.wsClient.create_workspace({'workspace': self.wsName})
        self.logger.info('set up new workspace: ' + self.wsName)

        # refdata WS
        self.refdata_wsName = 'test_kb_Msuite_refdata_1588183380977'
        self.refdata_ws_info = [
            49697,
            'test_kb_Msuite_refdata_1588183380977',
            'ialarmedalien',
            '2020-04-29T18:03:01+0000',
            0, 'a', 'n', 'unlocked', {}
        ]

        self.au = AssemblyUtil(os.environ['SDK_CALLBACK_URL'])
        self.gfu = GenomeFileUtil(os.environ['SDK_CALLBACK_URL'], service_ver='dev')
        self.kr = KBaseReport(os.environ['SDK_CALLBACK_URL'])
        self.mu = MetagenomeUtils(os.environ['SDK_CALLBACK_URL'])
        self.setAPI = SetAPI(url=self.cfg['srv-wiz-url'], token=self.ctx['token'])

        self.data_loaded = False

        self.github_run = False
        if os.path.exists(os.path.join(self.appdir, 'running_on_github.txt')):
            self.github_run = True

        # copy test data to /tmp
        os.makedirs('/kb/module/work/tmp/test_data', exist_ok=True)
        shutil.copytree(
            os.path.join('/kb', 'module', 'test', 'data'),
            os.path.join('/kb', 'module', 'work', 'tmp', 'test_data')
        )
        for file in os.listdir(os.path.join('/kb', 'module', 'work', 'tmp', 'test_data')):
            self.logger.info('file: ' + file)

        self.test_data_dir = os.path.join(self.scratch, 'test_data')
        os.makedirs(self.test_data_dir, exist_ok=True)

        self.logger.info('Finished env set up')

        self.env_set_up = True
