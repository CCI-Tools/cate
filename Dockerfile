FROM quay.io/bcdev/xcube:v0.11.2

LABEL maintainer="tonio.fincke@brockmann-consult.de"
LABEL name=cate

ARG XCUBE_USER_NAME=xcube

USER root

RUN apt-get update -y && apt-get upgrade -y

USER ${XCUBE_USER_NAME}

# STAGE LINUX/CONDA BASICS
SHELL ["/bin/bash", "-c"]

WORKDIR /tmp

# INSTALL software

# cate

COPY --chown=1000:1000 . ./cate

WORKDIR /tmp/cate

RUN source activate xcube && python setup.py install

# Install missing dependencies

RUN source activate xcube && mamba install -c conda-forge cartopy

WORKDIR /home/${XCUBE_USER_NAME}

CMD ["/bin/bash"]
