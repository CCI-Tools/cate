FROM quay.io/ccitools/cate-base:2.1.0

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=cate

ARG CATE_USER_NAME=cate
ENV XCUBE_VERSION=0.8.2.dev0
ENV XCUBE_CCI_VERSION=0.8.1.dev1

USER root

RUN apt-get update -y && apt-get upgrade -y

USER ${CATE_USER_NAME}

# STAGE LINUX/CONDA BASICS
SHELL ["/bin/bash", "-c"]

WORKDIR /tmp

# STAGE INSTALL CATE DEPENDENCIES

USER ${CATE_USER_NAME}

# INSTALL cate

COPY --chown=1000:1000 . ./
# Removed xcube dependencies. Will be manually installed
RUN sed -i 's/- xcube/# -xcube/g' environment.yml
RUN mamba env create
RUN source activate cate-env && python setup.py install

# INSTALL xcube

RUN wget https://github.com/dcs4cop/xcube/archive/v"${XCUBE_VERSION}".tar.gz
RUN tar xvzf v"${XCUBE_VERSION}".tar.gz

WORKDIR /tmp/xcube-"${XCUBE_VERSION}"

RUN mamba env update -n cate-env
RUN source activate cate-env && python setup.py install

# Install xcube-cci

WORKDIR /tmp

RUN wget https://github.com/dcs4cop/xcube-cci/archive/v"${XCUBE_CCI_VERSION}".tar.gz
RUN tar xvzf v"${XCUBE_CCI_VERSION}".tar.gz

WORKDIR /tmp/xcube-cci-"${XCUBE_CCI_VERSION}"
RUN source activate cate-env && python setup.py install

RUN source activate cate-env && mamba install -y -c conda-forge aiohttp nest-asyncio lxml pydap

WORKDIR /home/${CATE_USER_NAME}

CMD ["/bin/bash"]
