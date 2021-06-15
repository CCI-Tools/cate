FROM quay.io/ccitools/cate-base:2.1.0

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=cate

ARG CATE_USER_NAME=cate
ENV XCUBE_CCI_INSTALL_MODE=github
ENV XCUBE_CCI_VERSION=0.8.0

USER root

RUN apt-get update -y && apt-get upgrade -y

USER ${CATE_USER_NAME}

# STAGE LINUX/CONDA BASICS
SHELL ["/bin/bash", "-c"]

WORKDIR /tmp

# STAGE INSTALL CATE DEPENDENCIES

USER ${CATE_USER_NAME}

COPY environment.yml ./
RUN mamba env create
RUN conda info --envs
RUN source activate cate-env && conda list

# STAGE INSTALL CATE

COPY --chown=1000:1000 . ./
RUN source activate cate-env && pip install .

# Install xcube-cci

RUN wget https://github.com/dcs4cop/xcube-cci/archive/v"${XCUBE_CCI_VERSION}".tar.gz
RUN tar xvzf v"${XCUBE_CCI_VERSION}".tar.gz

WORKDIR xcube-cci-"${XCUBE_CCI_VERSION}"
RUN mamba env update -n cate-env
RUN source activate cate-env
RUN python setup.py install

WORKDIR /home/${CATE_USER_NAME}

CMD ["/bin/bash"]
