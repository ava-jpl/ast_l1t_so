FROM hysds/gdal-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Generate products"

RUN sudo pip install rasterio

USER ops

COPY . /home/ops/verdi/ops/ast_l1t_so

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

