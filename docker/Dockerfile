FROM quay.io/ccitools/cate-base:2.1.0

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=cate

# STAGE LINUX/CONDA BASICS
SHELL ["/bin/bash", "-c"]


ARG CATE_USER_NAME=cate
ENV XCUBE_CCI_INSTALL_MODE=github
ENV XCUBE_CCI_VERSION=0.8.0

WORKDIR /tmp

# STAGE INSTALL CATE DEPENDENCIES

USER ${CATE_USER_NAME}

ADD . ./
RUN mamba env create
RUN conda info --envs
RUN source activate cate-env && conda list

# STAGE INSTALL CATE

ADD --chown=1000:1000 . cate
RUN source activate cate-env && cd cate && pip install .

RUN bash ./docker/install_xcube_cci.sh

WORKDIR /home/${CATE_USER_NAME}

CMD ["/bin/bash"]
