from installed_clients.AssemblyUtilClient import AssemblyUtil
from installed_clients.MetagenomeUtilsClient import MetagenomeUtils
from installed_clients.SetAPIServiceClient import SetAPI
from installed_clients.WorkspaceClient import Workspace

from kb_Msuite.Utils.ClientUtil import ClientUtil

from CheckMTestBase import CoreCheckMTestClient


class TestClientUtil(CoreCheckMTestClient):

    def test_init_client(self):

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

    def test_init_checkMUtil_client(self):

        cmu = self.checkMUtil

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

    def test_client_with_command(self):

        cmu = self.checkMUtil

        err_str = 'KBaseReport cannot perform the command "go_your_own_way"'
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.client('KBaseReport', 'go_your_own_way')

        # attribute, not callable
        err_str = 'KBaseReport cannot perform the command "_service_ver"'
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.client('KBaseReport', '_service_ver')

        # no args
        result = cmu.client('KBaseReport', 'status')
        self.assertEqual(result[status], 'OK')

    # def test_client_with_command_and_args(self):

    #     cmu = self.checkMUtil
