#!/bin/bash

set -e

#ln -s /etc/supervisor/supervisord.conf /etc/supervisord.conf

exec /usr/bin/supervisord -c /etc/supervisor/supervisord.conf --nodaemon