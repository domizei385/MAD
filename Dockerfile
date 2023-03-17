############################
# MAD
############################
FROM python:3.11-slim AS mad-core
# Working directory for the application
WORKDIR /usr/src/app

# copy requirements only, to reduce image size and improve cache usage
COPY requirements.txt /usr/src/app/

# Install required system packages + python requirements + cleanup in one layer (yields smaller docker image).
# If you try to debug the build you should split into single RUN commands
RUN export DEBIAN_FRONTEND=noninteractive && apt-get update \
&& apt-get install -y --no-install-recommends \
build-essential \
default-libmysqlclient-dev \
pkg-config \
libmariadb-dev \
# OpenCV & dependencies \
python3-opencv \
libsm6 \
libgl1-mesa-glx \
# tesseract-ocr needs to be up here since the apt-get update will make it visible
tesseract-ocr \
supervisor \
&& update-rc.d supervisor defaults \
# python reqs
&& python3 -m pip install --no-cache-dir -r requirements.txt ortools redis \
# cleanup \
&& apt-get remove -y build-essential \
&& apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
&& rm -rf /var/lib/apt/lists/*

RUN export DEBIAN_FRONTEND=noninteractive && apt-get update \
&& apt-get install -y --no-install-recommends nginx  \
&& usermod -a -G root www-data \
&& sed -i 's/user www-data/user root/g' /etc/nginx/nginx.conf

# Set Entrypoint with hard-coded options
ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Default ports for PogoDroid, RGC and MAdmin
EXPOSE 8080 8000 5000

# Copy everything to the working directory (Python files, templates, config) in one go.
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
COPY . /usr/src/app/