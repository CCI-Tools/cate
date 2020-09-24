ARG MINICONDA_VERSION=4.8.2

FROM continuumio/miniconda3:${MINICONDA_VERSION}

ARG CATE_USER_NAME=cate

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=cate

# STAGE LINUX/CONDA BASICS
SHELL ["/bin/bash", "-c"]

USER root
RUN apt-get -y update && apt-get -y install vim

RUN groupadd -g 1000 ${CATE_USER_NAME}
RUN useradd -u 1000 -g 1000 -ms /bin/bash ${CATE_USER_NAME}
RUN mkdir /workspace && chown ${CATE_USER_NAME}.${CATE_USER_NAME} /workspace
RUN chown -R ${CATE_USER_NAME}.${CATE_USER_NAME} /opt/conda

USER ${CATE_USER_NAME}

RUN source activate base && conda update -n base conda && conda init
RUN source activate base && conda install -y -c conda-forge mamba

RUN echo "conda activate cate-env" >> ~/.bashrc

WORKDIR /tmp

# STAGE INSTALL CATE DEPENDENCIES

ADD --chown=1000:1000 ./environment.yml cate-${CATE_VERSION}/environment.yml
RUN mamba env create -f cate-${CATE_VERSION}/environment.yml
RUN conda info --envs
RUN source activate cate-env && conda list

# STAGE INSTALL CATE

ADD --chown=1000:1000 . cate-${CATE_VERSION}
RUN source activate cate-env && cd cate-${CATE_VERSION} && pip install .

WORKDIR /home/${CATE_USER_NAME}

ENTRYPOINT ["/bin/bash"]
