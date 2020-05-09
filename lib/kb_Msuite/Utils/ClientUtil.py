from installed_clients.AssemblyUtilClient import AssemblyUtil
from installed_clients.DataFileUtilClient import DataFileUtil
from installed_clients.KBaseReportClient import KBaseReport
from installed_clients.MetagenomeUtilsClient import MetagenomeUtils
from installed_clients.SetAPIServiceClient import SetAPI
from installed_clients.WorkspaceClient import Workspace


class ClientUtil:

    def __init__(self, config):
        ''' requires config in the form
        {
            'token': self.ctx['token'],
            'workspaceURL': config['workspace-url'],
            'serviceWizardURL': config['srv-wiz-url'],
            'callback_url': config['SDK_CALLBACK_URL'],
        }
        '''
        attr_list = ['callback_url', 'service_wizard_url', 'token', 'workspace_url']

        for attr in attr_list:
            if attr not in config or not config[attr]:
                raise ValueError('Missing required ClientUtil config value: ' + attr)
            setattr(self, attr, config[attr])

        self.service_ver = 'release'

    def init_AssemblyUtil(self):

        return AssemblyUtil(self.callback_url, token=self.token, service_ver=self.service_ver)

    def init_DataFileUtil(self):

        return DataFileUtil(self.callback_url)

    def init_KBaseReport(self):

        return KBaseReport(self.callback_url)

    def init_MetagenomeUtils(self):

        return MetagenomeUtils(self.callback_url, token=self.token, service_ver=self.service_ver)

    def init_SetAPI(self):

        return SetAPI(url=self.service_wizard_url, token=self.token)

    def init_Workspace(self):

        return Workspace(self.workspace_url)

    def client(self, client, *args):

        client_mapping = {
            'AssemblyUtil':     self.init_AssemblyUtil,
            'DataFileUtil':     self.init_DataFileUtil,
            'KBaseReport':      self.init_KBaseReport,
            'MetagenomeUtils':  self.init_MetagenomeUtils,
            'SetAPI':           self.init_SetAPI,
            'Workspace':        self.init_Workspace,
        }

        if client not in client_mapping:
            raise ValueError(client + ' client does not exist')

        if not hasattr(self, '_' + client):
            try:
                setattr(self, '_' + client, client_mapping[client]())
            except Exception as e:
                raise ValueError('Error instantiating ' + client + ' client: ' + str(e))

        return getattr(self, '_' + client)
