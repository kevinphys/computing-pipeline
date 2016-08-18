# Dockerfile for hyperspectral image conversion extractor development
To build:
<pre>
docker-compose build hyperspectral
</pre>

To run the container:
<pre>
docker-compose up hyperspectral
</pre>

This docker container already has a sample input dataset downloaded. To test `terraref.sh`, get a terminal to the running docker, then:
<pre>
/bin/bash
cd $HOME
source ./.softenv
. ./pyenv/bin/activate
cd computing-pipeline/scripts/hyperspectral/
./terraref.sh -d 1 -I ~/terraref-hyperspectral-input-sample -O ~/output
gdalinfo ~/output/${netcdf_filename}
ncks --cdl -m -M ~/output/${netcdf_filename}
</pre>
