import unittest
import logging
from TestEngine import TestEngine

from test_BinnedContigFilter import TestBinnedContigFilter
from test_checkM_end_to_end import TestCheckMEndToEnd
from test_CheckMUtil import TestCheckMUtil
from test_ClientUtil import TestClientUtil
from test_DataStagingUtils import TestDataStagingUtils
from test_FileUtils import TestFileUtils
from test_OutputBuilder import TestOutputBuilder
from test_WorkspaceHelper import TestWorkspaceHelper

if __name__ == '__main__':

    print("RUNNING MAIN!!!")

    # init logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(name)s %(levelname)s %(message)s'
    )

    te = TestEngine()
    te.set_up_test_env()
    te.logger.info('test env status: ' + te.env_set_up)

    test_loader = unittest.TestLoader()
    test_classes = [
        TestBinnedContigFilter,
        TestCheckMEndToEnd,
        TestCheckMUtil,
        TestClientUtil,
        TestDataStagingUtils,
        TestFileUtils,
        TestOutputBuilder,
        TestWorkspaceHelper
    ]

    suites = [test_loader.loadTestsFromTestCase(test_class) for test_class in test_classes]
    all_tests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=3).run(all_tests)
