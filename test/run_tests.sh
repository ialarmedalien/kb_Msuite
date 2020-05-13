#!/bin/bash
echo "Running $0 with args $@"
if [ -L $0 ] ; then
    script_dir=$(cd "$(dirname "$(readlink $0)")"; pwd -P) # for symbolic link
else
    script_dir=$(cd "$(dirname "$0")"; pwd -P) # for normal file
fi
export APPDIR=/kb/module
export KB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg
export KB_AUTH_TOKEN=`cat /kb/module/work/token`
export PYTHONPATH=$APPDIR/lib:$APPDIR/test:$PYTHONPATH
cd $script_dir/..
python -m compileall lib/ test/
cd $script_dir/../test
rm -rf tmp
mkdir -p tmp
PYTHONPATH=$APPDIR/lib/:$APPDIR/test/:$PYTHONPATH coverage run -m unittest -v kb_Msuite_testSuite.py
#python -m nose --with-coverage --cover-package=kb_Msuite --cover-html --cover-html-dir=/kb/module/work/test_coverage --nocapture  --nologcapture .
