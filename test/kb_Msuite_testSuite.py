import unittest
from test_BinnedContigFilter import TestBinnedContigFilter
from test_checkM_end_to_end import TestCheckMEndToEnd
from test_CheckMUtil import TestCheckMUtil
from test_ClientUtil import TestClientUtil
from test_DataStagingUtils import TestDataStagingUtils
from test_FileUtils import TestFileUtils
from test_OutputBuilder import TestOutputBuilder
from test_WorkspaceHelper import TestWorkspaceHelper

if __name__ == '__main__':
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

    suites = []
    for test_class in test_classes:
        suite = test_loader.loadTestsFromTestCase(test_class)
        suites.append(suite)

    all_tests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(all_tests)
