ARG CATE_VERSION=2.1.0.dev6

FROM quay.io/bcdev/cate-python-deps:${CATE_VERSION}

ARG CATE_VERSION=helge
ARG CATE_DOCKER_VERSION
ARG CATE_USER_NAME=cate

LABEL maintainer="helge.dzierzon@brockmann-consult.de"
LABEL name=cate
LABEL cate_version=${CATE_VERSION}
LABEL cate_docker_version=${CATE_DOCKER_VERSION}

RUN echo "Building docker using args:"
RUN echo "CATE_VERSION:${CATE_VERSION}"

USER ${CATE_USER_NAME}
#RUN wget https://github.com/CCI-Tools/cate/archive/v${CATE_VERSION}.tar.gz
#RUN tar xvf v${CATE_VERSION}.tar.gz
WORKDIR /workspace/cate
ADD . cate
#RUN source activate cate-env && cd cate-${CATE_VERSION} && pip install .

RUN cat cate/version.py
RUN source activate cate-env && pip install .
RUN source activate cate-env && mamba install -y jupyterhub=0.9.6 jupyterlab
CMD ["/bin/bash", "-c", "source activate cate-env && cate-webapi-start -v -p 4000 -a 0.0.0.0"]
