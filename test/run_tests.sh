#!/bin/bash
echo "Running $0 with args $@"
script_dir=$(dirname "$(readlink -f "$0")")
export KB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg
export KB_AUTH_TOKEN=`cat /kb/module/work/token`
export PYTHONPATH=$script_dir/../lib:$script_dir/../test:$PYTHONPATH
cd $script_dir/..
python -m compileall lib/ test/
# cd $script_dir/../test
rm -rf tmp
mkdir -p tmp
coverage run -m unittest -v --locals test/kb_Msuite_testSuite.py
#python -m nose --with-coverage --cover-package=kb_Msuite --cover-html --cover-html-dir=/kb/module/work/test_coverage --nocapture  --nologcapture .
