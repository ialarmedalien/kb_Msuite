from CheckMTestBase import CoreCheckMTestClient


class TestWorkspaceHelper(CoreCheckMTestClient):

    @classmethod
    def setUpClass(self):
        super().setUpClass()

        # create a report
        self.report_params = {
            'workspace_name': self.ws_info[1],
            'report_object_name': 'Super_Cool_Extended_Report',
            'message': 'This is the best report in the world',
        }

        # {'ref': '49674/1/1', 'name': 'Super_Cool_Extended_Report'}
        report_output = self.kr.create_extended_report(self.report_params)
        self.test_report_reference = report_output['ref']

        # [1, 'Super_Cool_Extended_Report', 'KBaseReport.Report-3.0', '2020-05-08T22:15:44+0000', 1,
        # '***', 50109, 'test_kb_Msuite_*', '72c0c1862c986bfd8e9dc44d003be88a', 226, None]
        self.ws_obj_info = self.wsClient.get_object_info3({
            'objects': [{'ref': self.test_report_reference}]
        })
        # KBaseReport object
        self.ws_obj = self.wsClient.get_objects2({
            'objects': [{'ref': self.test_report_reference}]
        })

    def test_00_workspace_helper(self):

        self.logger.info("=================================================================")
        self.logger.info("RUNNING 00_workspace_helper")
        self.logger.info("=================================================================\n")

        # _run_workspace_command(self, command, args):
        self.assertTrue('Oh shit.')
        # TEST ME!
        # try:
        #     # get the workspace method and call it with the provided args
        #     method = getattr(self.client('Workspace'), command)
        #     result = method(args)
        # except Exception as e:
        #     err_str = 'Unable to perform workspace command ' + command + ': ' + str(e)
        #     raise ValueError(err_str)
        #     # to get the full stack trace: traceback.format_exc()

    def test_get_objects_from_workspace(self):

        cmu = self.checkMUtil
        ws_objs = cmu.workspacehelper.get_obj_from_workspace(self.test_report_reference)

        self.assertEqual(ws_objs, self.ws_obj['data'])
        self.assertEqual(
            ws_objs[0]['data'],
            cmu.workspacehelper.get_obj_from_workspace(self.test_report_reference)
        )

    def test_get_obj_from_workspace(self):

        cmu = self.checkMUtil
        ws_obj = cmu.workspacehelper.get_obj_from_workspace(self.test_report_reference)

        self.assertEqual(ws_obj['text_message'], self.report_params['message'])
        self.assertEqual(ws_obj, self.ws_obj['data'][0]['data'])

        err_str = 'Unable to perform workspace command get_objects2: '
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.workspacehelper.get_obj_from_workspace('ROTFLMAO')

    def test_get_ws_obj_info(self):

        cmu = self.checkMUtil
        obj_info = cmu.workspacehelper.get_ws_obj_info(self.test_report_reference)
        self.assertEqual(obj_info, self.ws_obj_info['infos'][0])

        # invalid ref
        err_str = 'Unable to perform workspace command get_object_info3: '
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.workspacehelper.get_ws_obj_name('the_best_object_ever')

    def test_get_object_property(self):

        cmu = self.checkMUtil

        # obviously these tests are dependent on get_ws_obj_info being correct
        obj_info = cmu.workspacehelper.get_ws_obj_info(self.test_report_reference)

        obj_name = cmu.workspacehelper.get_object_property(obj_info, 'name')
        self.assertEqual(obj_name, self.report_params['report_object_name'])

        obj_type = cmu.workspacehelper.get_object_property(obj_info, 'type')
        self.assertEqual(obj_type, 'KBaseReport.Report-3.0')

        err_str = 'personality is not a valid workspace object property'
        with self.assertRaisesRegex(KeyError, err_str):
            cmu.workspacehelper.get_object_property(obj_info, 'personality')

    def test_get_ws_obj_name(self):

        cmu = self.checkMUtil

        err_str = "Must supply either ref or object_info to get_ws_obj_name"
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.workspacehelper.get_ws_obj_name()

        obj_name = cmu.workspacehelper.get_ws_obj_name(self.test_report_reference)
        self.assertEqual(obj_name, self.report_params['report_object_name'])

        obj_info = cmu.workspacehelper.get_ws_obj_info(self.test_report_reference)
        obj_name = cmu.workspacehelper.get_ws_obj_name(object_info=obj_info)
        self.assertEqual(obj_name, self.report_params['report_object_name'])

    def test_get_ws_obj_type(self):

        cmu = self.checkMUtil

        err_str = "Must supply either ref or object_info to get_ws_obj_type"
        with self.assertRaisesRegex(ValueError, err_str):
            cmu.workspacehelper.get_ws_obj_type(remove_module=True)

        obj_type = cmu.workspacehelper.get_ws_obj_type(self.test_report_reference)
        self.assertEqual(obj_type, 'KBaseReport.Report')

        obj_info = cmu.workspacehelper.get_ws_obj_info(self.test_report_reference)
        obj_type = cmu.workspacehelper.get_ws_obj_type(
            object_info=obj_info, remove_module=False
        )
        self.assertEqual(obj_type, 'KBaseReport.Report')

        obj_type = cmu.workspacehelper.get_ws_obj_type(
            ref=self.test_report_reference, remove_module=True
        )
        self.assertEqual(obj_type, 'Report')

        obj_type = cmu.workspacehelper.get_ws_obj_type(
            object_info=self.obj_info, remove_module=True
        )
        self.assertEqual(obj_type, 'Report')
