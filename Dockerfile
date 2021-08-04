FROM quay.io/bcdev/xcube-python-base:0.8.1

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=cate

ARG XCUBE_USER_NAME=xcube
ENV XCUBE_VERSION=0.8.2.dev0
ENV XCUBE_CCI_VERSION=0.8.1.dev6

USER root

RUN apt-get update -y && apt-get upgrade -y

USER ${XCUBE_USER_NAME}

# STAGE LINUX/CONDA BASICS
SHELL ["/bin/bash", "-c"]

WORKDIR /tmp

# STAGE INSTALL CATE DEPENDENCIES

# Prepare Python conda env

# xcube

WORKDIR /tmp

RUN wget https://github.com/dcs4cop/xcube/archive/v"${XCUBE_VERSION}".tar.gz
RUN tar xvzf v"${XCUBE_VERSION}".tar.gz

WORKDIR /tmp/xcube-"${XCUBE_VERSION}"

RUN mamba env create -n cate-env

# xcube-cci

WORKDIR /tmp

RUN source activate cate-env && mamba install -y -c conda-forge aiohttp nest-asyncio lxml pydap cartopy

# INSTALL software

# cate# Start bash, so we can invoke xcube CLI.


COPY  . ./cate
RUN chown -R 1000.1000 /tmp/cate
WORKDIR /tmp/cate

RUN source activate cate-env && python setup.py install

# xcube

WORKDIR /tmp/xcube-"${XCUBE_VERSION}"
RUN source activate cate-env && python setup.py install

# xcube-cci

WORKDIR /tmp
RUN wget https://github.com/dcs4cop/xcube-cci/archive/v"${XCUBE_CCI_VERSION}".tar.gz
RUN tar xvzf v"${XCUBE_CCI_VERSION}".tar.gz

WORKDIR /tmp/xcube-cci-"${XCUBE_CCI_VERSION}"
RUN source activate cate-env && python setup.py install

WORKDIR /home/${CATE_USER_NAME}

CMD ["/bin/bash"]
