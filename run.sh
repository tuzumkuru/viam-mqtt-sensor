#!/usr/bin/env bash

set -euxo pipefail

# setup environment variables
# parameter substitution to support running outside of a module 
export DATA_DIR=${VIAM_MODULE_DATA:=$(dirname $0)}
export VIRTUAL_ENV=$DATA_DIR/.venv
export PYTHON=$VIRTUAL_ENV/bin/python

./setup.sh

exec $PYTHON -m src.main $@