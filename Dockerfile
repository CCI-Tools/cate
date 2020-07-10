FROM continuumio/miniconda3

ARG CATE_VERSION=dev
ARG CATE_USER_NAME=cate

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name="cate miniconda base"
LABEL cate_version=${CATE_VERSION}
LABEL cate_docker_version=${CATE_DOCKER_VERSION}

USER root
RUN apt-get -y update && apt-get -y install vim

SHELL ["/bin/bash", "-c"]
RUN groupadd -g 1000 ${CATE_USER_NAME}
RUN useradd -u 1000 -g 1000 -ms /bin/bash ${CATE_USER_NAME}
RUN mkdir /workspace && chown ${CATE_USER_NAME}.${CATE_USER_NAME} /workspace

RUN chown -R ${CATE_USER_NAME}.${CATE_USER_NAME} /opt/conda

USER ${CATE_USER_NAME}

WORKDIR /home/${CATE_USER_NAME}

RUN source activate base && conda update -n base conda && conda init
RUN source activate base && conda install -y -c conda-forge mamba

USER ${CATE_USER_NAME}

ADD --chown=1000:1000 environment.yml environment.yml
RUN mamba env create -f environment.yml

RUN source activate cate-env && mamba install -y jupyterhub=0.9.6 jupyterlab

ADD --chown=1000:1000 ./ .
RUN source activate cate-env && pip install .

WORKDIR /workspace

EXPOSE 8888

CMD ["/bin/bash", "-c", "source activate cate-env && cate-webapi-start -v -p 8888 -a 0.0.0.0"]