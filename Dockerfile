FROM continuumio/miniconda3:latest

# Person responsible
MAINTAINER helge.dzierzon@brockmann-consult.de

LABEL name=cate
LABEL version=0.2.0
LABEL conda_env=cate

# Ensure usage of bash (simplifies source activate calls)
SHELL ["/bin/bash", "-c"]

# Update system and install dependencies
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install sendmail

# Setup conda environment
# Copy yml config into image
ADD environment.yml /tmp/environment.yml

# Update conda and install dependecies specified in environment.yml
RUN  conda update -n base conda
RUN  conda env create -f=/tmp/environment.yml

# Set work directory for eocdb installation
RUN mkdir /cate-env
WORKDIR /cate-env

# Copy local github repo into image (will be replaced by either git clone or as a conda dep)
ADD . /cate-env

# Setup eocdb-dev
RUN source activate cate-env; \
    python setup.py develop

# Export web server port 4000
EXPOSE 4000

# Start server

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["source activate cate-env && cate-webapi-start" ]
