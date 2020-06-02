#!/usr/bin/env bash
#
# Copyright 2017-2020 GridGain Systems.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


##############################################################################
# Prepare artifacts config files to run with artifacts from specific working
# directory by patching inplace './work/' to given path
#
# Usage:
#   bash \
#       utils/prepare_artifacts_config.sh \
#           --work-dir=<WORK_DIR> \
#           --config=config/jenkins-artifacts-gg-ult-fab.yaml \
#           [--config=...]
#
##############################################################################

WORK_DIR=${WORK_DIR:-}
VAR_DIR=${VAR_DIR:-}

function patch_config()
{
    local config_name="$1"

    local config_filename=$(basename $config_name)

    if [ ! -f $config_name ]; then
        echo "Can't find configuration file $config_name"
        exit 2
    fi
    cat $config_name \
        | sed 's/\.\/work/'${WORK_DIR//\//\\\/}'/g' \
        > $WORK_DIR/$config_filename
}

while [ $# -gt 0 ]; do
    opt="$1"
    if [ "$(echo "$opt" | cut -d'=' -f 1)" = "$1" ]; then
        if [[ "$2" = "--"* ]]; then
            val=""
        else
            val="$2"
            shift
        fi
        shift
    else
        val="$(echo "$opt" | cut -d'=' -f 2-)"
        opt="$(echo "$opt" | cut -d'=' -f 1)"
        shift
    fi

    if [ "$opt" = "--work-dir" ]; then
        WORK_DIR="$val"
        continue
    fi
    if [ "$opt" = "--var-dir" ]; then
        VAR_DIR="$val"
        continue
    fi
    if [ "$opt" = '--config' ]; then
        patch_config "$val"
        continue
    fi
    echo "Unknown option: $opt"
    exit 1
done
