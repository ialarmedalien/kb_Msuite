#!/bin/bash

. /kb/deployment/user-env.sh

python ./scripts/prepare_deploy_cfg.py ./deploy.cfg ./work/config.properties

if [ -f ./work/token ] ; then
  export KB_AUTH_TOKEN=$(<./work/token)
elif [ ! -z ${KBASE_TEST_TOKEN} ] ; then
  # put the test token into work/token
  cat <<< ${KBASE_TEST_TOKEN} > work/token
  export KB_AUTH_TOKEN=${KBASE_TEST_TOKEN}
fi


if [ $# -eq 0 ] ; then
  sh ./scripts/start_server.sh
elif [ "${1}" = "test" ] ; then
  echo "Run Tests"
  make test
elif [ "${1}" = "async" ] ; then
  sh ./scripts/run_async.sh
elif [ "${1}" = "init" ] ; then
  echo "Initialize module"

# CheckM relies on a number of precalculated data files which can be downloaded from https://data.ace.uq.edu.au/public/CheckM_databases/. Decompress the file to an appropriate folder and run the following to inform CheckM of where the files have been placed:

# > checkm data setRoot <checkm_data_dir>


# RUN wget https://data.ace.uq.edu.au/public/CheckM_databases/checkm_data_2015_01_16.tar.gz

# For checkm-genome required data
# RUN \
#     mkdir /data \
#     && touch /data/DATA_CONFIG \
#     && mkdir -p /data/checkm_data \
#     && mv /miniconda/lib/python3.6/dist-packages/checkm/DATA_CONFIG /miniconda/lib/python3.6/dist-packages/checkm/DATA_CONFIG.orig \
#     && cp /miniconda/lib/python3.6/dist-packages/checkm/DATA_CONFIG.orig /data/DATA_CONFIG \
#     && ln -sf /data/DATA_CONFIG /miniconda/lib/python3.6/dist-packages/checkm/DATA_CONFIG

  mkdir /data/checkm_data
  cd /data/checkm_data
  echo "downloading: https://data.ace.uq.edu.au/public/CheckM_databases/checkm_data_2015_01_16.tar.gz"
  wget -q https://data.ace.uq.edu.au/public/CheckM_databases/checkm_data_2015_01_16.tar.gz
  tar -xzf checkm_data_2015_01_16.tar.gz
  rm -r checkm_data_2015_01_16.tar.gz
  echo /data/checkm_data | checkm data setRoot /data/checkm_data
  echo y | checkm data update # ensure you have the latest (32) data files from the ACE server
  if [ -d "/data/checkm_data/genome_tree" ] ; then
    touch /data/__READY__
  else
    echo "Init failed"
  fi
elif [ "${1}" = "bash" ] ; then
  bash
elif [ "${1}" = "report" ] ; then
  export KB_SDK_COMPILE_REPORT_FILE=./work/compile_report.json
  make compile
else
  echo Unknown
fi
