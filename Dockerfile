FROM kbase/sdkbase2:python
MAINTAINER KBase Developer
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.

# install cython that pysam likes
RUN apt-get update && apt-get install -y build-essential wget
RUN pip install --upgrade pip setuptools Cython==0.25

###### CheckM installation
#  Directions from https://github.com/Ecogenomics/CheckM/wiki/Installation#how-to-install-checkm

# CheckM requires the following programs to be added to your system path:
#
# HMMER (>=3.1b1)
# prodigal (2.60 or >=2.6.1)
# executable must be named prodigal and not prodigal.linux
# pplacer (>=1.1)
# guppy, which is part of the pplacer package, must also be on your system path
# pplacer binaries can be found on the pplacer GitHub page

# Install HMMER (>=3.1b1)
WORKDIR /kb/module
RUN \
  curl http://eddylab.org/software/hmmer3/3.1b2/hmmer-3.1b2-linux-intel-x86_64.tar.gz > hmmer-3.1b2-linux-intel-x86_64.tar.gz && \
  tar -zxvf hmmer-3.1b2-linux-intel-x86_64.tar.gz && \
  ln -s hmmer-3.1b2-linux-intel-x86_64 hmmer && \
  rm -f hmmer-3.1b2-linux-intel-x86_64.tar.gz && \
  cd hmmer && \
  ./configure && \
  make && make install && \
  cd easel && make check && make install


# Install Prodigal (2.60 or >=2.6.1)
WORKDIR /kb/module
RUN \
  wget https://github.com/hyattpd/Prodigal/archive/v2.6.3.tar.gz && \
  tar -zxvf v2.6.3.tar.gz && \
  ln -s Prodigal-2.6.3 prodigal && \
  rm -f v2.6.3.tar.gz && \
  cd prodigal && \
  make && \
  cp prodigal /kb/deployment/bin/prodigal

# Install Pplacer (>=1.1)
WORKDIR /kb/module
RUN \
  wget https://github.com/matsen/pplacer/releases/download/v1.1.alpha19/pplacer-linux-v1.1.alpha19.zip && \
  unzip pplacer-linux-v1.1.alpha19.zip && \
  ln -s pplacer-Linux-v1.1.alpha19 pplacer && \
  rm -f pplacer-linux-v1.1.alpha19.zip && \
  rm -f pplacer-1.1.alpha19.tar.gz && \
  cp -R pplacer-Linux-v1.1.alpha19 /kb/deployment/bin/pplacer

ENV PATH "$PATH:/kb/deployment/bin/pplacer"

# This will install CheckM and all other required Python libraries.
RUN \
    pip3 install numpy \
    && pip3 install matplotlib==3.1.0 \
    && pip3 install pysam \
    && pip3 install scipy \
    && pip3 install dendropy

RUN pip3 install checkm-genome \
    && cp -R /miniconda/bin/checkm /kb/deployment/bin/CheckMBin

RUN \
    mkdir -p /data/checkm_data \
    && mv /miniconda/lib/python3.6/site-packages/checkm/DATA_CONFIG /miniconda/lib/python3.6/site-packages/checkm/DATA_CONFIG.orig \
    && touch /data/DATA_CONFIG \
    && cp /miniconda/lib/python3.6/site-packages/checkm/DATA_CONFIG.orig /data/DATA_CONFIG \
    && ln -sf /data/DATA_CONFIG /miniconda/lib/python3.6/site-packages/checkm/DATA_CONFIG \
    && chmod +rwx /data/DATA_CONFIG

# -----------------------------------------
COPY ./ /kb/module

WORKDIR /kb/module

RUN mkdir -p /kb/module/work \
    && chmod -R a+rw /kb/module \
    && make all \
    && rm /data/__READY__

# RUN mkdir -p checkm_data \
#     && cp checkm_data_2015_01_16.tar.gz checkm_data/checkm_data_2015_01_16.tar.gz \
#     && cd checkm_data \
#     && tar -xzf checkm_data_2015_01_16.tar.gz \
#     && rm -r checkm_data_2015_01_16.tar.gz \
#     && cd /kb/module \
#     && mv checkm_data/* /data/checkm_data/ \
#     && echo /data/checkm_data | checkm data setRoot /data/checkm_data \
#     && rm /data/__READY \
#     && ls -al /data/checkm_data \
#     && ls -al /data

# echo "Initialize module"
# cp /miniconda/lib/python3.6/site-packages/checkm/DATA_CONFIG.orig /data/DATA_CONFIG
# mkdir -p /data/checkm_data
# cd /data/checkm_data
# echo "downloading: https://data.ace.uq.edu.au/public/CheckM_databases/checkm_data_2015_01_16.tar.gz"
# wget https://data.ace.uq.edu.au/public/CheckM_databases/checkm_data_2015_01_16.tar.gz
# echo /data/checkm_data | checkm data setRoot /data/checkm_data
# #  echo y | checkm data update # ensure you have the latest (32) data files from the ACE server
# if [ -d "/data/checkm_data/genome_tree" ] ; then
#   touch /data/__READY__
# else
#   echo "Init failed"
# fi

WORKDIR /kb/module

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
