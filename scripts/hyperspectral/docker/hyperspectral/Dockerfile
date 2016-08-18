# Dockerfile for the TerraRef hyperspectral image conversion extractor
# August 17, 2016
FROM ubuntu:14.04
MAINTAINER Yan Y. Liu <yanliu@illinois.edu>

# install common libraries and python modules
USER root
RUN apt-get update
RUN apt-get upgrade -y -q 
RUN apt-get install -y -q build-essential m4 swig antlr libantlr-dev udunits-bin libudunits2-dev unzip cmake wget git libjpeg-dev libpng-dev libtiff-dev
RUN apt-get install -y -q python-dev python-numpy python-pip python-virtualenv
# set up dirs for user installed software
RUN useradd -m -s /bin/bash ubuntu
RUN mkdir /srv/downloads && chown -R ubuntu: /srv/downloads && \
    mkdir /srv/sw && chown -R ubuntu: /srv/sw

USER ubuntu
# set env vars for common libraries and python paths
ENV PYTHONPATH="/usr/lib/python2.7/dist-packages:${PYTHONPATH}"
## install from source 
# hdf5
RUN cd /srv/downloads && \
    wget -q https://www.hdfgroup.org/ftp/HDF5/releases/hdf5-1.8.17/src/hdf5-1.8.17.tar.gz && \
    tar xfz hdf5-1.8.17.tar.gz && \
    cd hdf5-1.8.17 && \
    ./configure --prefix=/srv/sw/hdf5-1.8.17 && \
    make && make install
ENV PATH="/srv/sw/hdf5-1.8.17/bin:${PATH}"
ENV LD_LIBRARY_PATH="/srv/sw/hdf5-1.8.17/lib:${LD_LIBRARY_PATH}"
# netcdf4
RUN cd /srv/downloads && \
    wget -q ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4.4.1.tar.gz && \
    tar xfz netcdf-4.4.1.tar.gz && \
    cd netcdf-4.4.1 && \
    CFLAGS="-I/srv/sw/hdf5-1.8.17/include " LDFLAGS=" -L/srv/sw/hdf5-1.8.17/lib " LIBS=" -lhdf5 -lhdf5_hl " ./configure --prefix=/srv/sw/netcdf-4.4.1 --enable-netcdf4 && \
    make && make install
ENV PATH="/srv/sw/netcdf-4.4.1/bin:${PATH}"
ENV LD_LIBRARY_PATH="/srv/sw/netcdf-4.4.1/lib:${LD_LIBRARY_PATH}"
# geos
RUN cd /srv/downloads && \
    wget -q http://download.osgeo.org/geos/geos-3.5.0.tar.bz2 && \
    tar xfj geos-3.5.0.tar.bz2 && \
    cd geos-3.5.0 && \
    ./configure --prefix=/srv/sw/geos --enable-python && \
    make && make install
ENV PATH="/srv/sw/geos/bin:${PATH}"
ENV PYTHONPATH="/srv/sw/geos/lib/python2.7/site-packages:${PYTHONPATH}"
ENV LD_LIBRARY_PATH="/srv/sw/geos/lib:${LD_LIBRARY_PATH}"
# proj4
RUN cd /srv/downloads && \
    wget -q https://github.com/OSGeo/proj.4/archive/4.9.2.tar.gz -O proj.4-4.9.2.tar.gz && \
    tar xfz proj.4-4.9.2.tar.gz && \
    cd proj.4-4.9.2 && \
    ./configure --prefix=/srv/sw/proj4  && \
    make && make install
ENV PATH="/srv/sw/proj4/bin:${PATH}"
ENV LD_LIBRARY_PATH="/srv/sw/proj4/lib:${LD_LIBRARY_PATH}"
# gdal
RUN cd /srv/downloads && \
    wget -q http://download.osgeo.org/gdal/2.1.1/gdal-2.1.1.tar.gz && \
    tar xfz gdal-2.1.1.tar.gz && \
    cd gdal-2.1.1 && \
    ./configure --with-libtiff=internal --with-geotiff=internal --with-png=internal --with-jpeg=internal --with-gif=internal --without-curl --with-python --with-hdf5=/srv/sw/hdf5-1.8.17 --with-netcdf=/srv/sw/netcdf-4.4.1 --with-geos=/srv/sw/geos/bin/geos-config --with-threads --prefix=/srv/sw/gdal && \
    make && make install
ENV PATH="/srv/sw/gdal/bin:${PATH}"
ENV PYTHONPATH="/srv/sw/gdal/lib/python2.7/site-packages:${PYTHONPATH}"
ENV LD_LIBRARY_PATH="/srv/sw/gdal/lib:${LD_LIBRARY_PATH}"
# nco
RUN cd /srv/downloads && \
    wget -q https://github.com/nco/nco/archive/4.6.1.tar.gz -O nco-4.6.1.tar.gz && \
    tar xfz nco-4.6.1.tar.gz && \
    cd nco-4.6.1 && \
    ./configure NETCDF_ROOT=/srv/sw/netcdf-4.4.1 --prefix=/srv/sw/nco-4.6.1 --enable-ncap2 --enable-udunits2 && \
    make && make install
ENV PATH="/srv/sw/nco-4.6.1/bin:${PATH}"
ENV LD_LIBRARY_PATH="/srv/sw/nco-4.6.1/lib:${LD_LIBRARY_PATH}"

## install hyperspectral image converter script
USER ubuntu
RUN cd ${HOME} && \
    git clone https://github.com/terraref/computing-pipeline.git && \
    mkdir -p ${HOME}/output && \
    mkdir -p ${HOME}/input && \
    mkdir -p ${HOME}/logs
# download test input data
RUN cd ${HOME} && \
    wget -q http://141.142.168.44/nfiedata/yanliu/terraref-hyperspectral-input-sample.tgz && \
    tar xfz terraref-hyperspectral-input-sample.tgz

## install pyclowder 
# install python modules
RUN cd ${HOME} && \
    virtualenv pyenv && \
    . pyenv/bin/activate && \
    pip install pika && \
    CC=gcc CXX=g++ USE_SETUPCFG=0 HDF5_INCDIR=/srv/sw/hdf5-1.8.17/include HDF5_LIBDIR=/srv/sw/hdf5-1.8.17/lib NETCDF4_INCDIR=/srv/sw/netcdf-4.4.1/include NETCDF4_LIBDIR=/srv/sw/netcdf-4.4.1/lib pip install netCDF4 && \
    pip install git+https://opensource.ncsa.illinois.edu/stash/scm/cats/pyclowder.git@bugfix/CATS-554-add-pyclowder-support-for-dataset && \
    deactivate

##TODO:  install extractor

USER root
COPY start.sh /home/ubuntu/
RUN chown ubuntu: /home/ubuntu/start.sh && \
    chmod 755 /home/ubuntu/start.sh 
COPY softenv /home/ubuntu/.softenv
RUN chown ubuntu: /home/ubuntu/.softenv

USER ubuntu
WORKDIR /home/ubuntu
CMD ["./start.sh"]


