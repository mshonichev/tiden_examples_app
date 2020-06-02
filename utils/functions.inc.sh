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

# Load .yaml-style config and set bash variables from it
function load_config()
{
    local yaml_config="$1"
    local var_name=
    local var_value=
    if [ "$yaml_config" = "" ]; then
        echo "Config name required!"
        exit 1
    fi
    if [ ! -s "$yaml_config" ]; then
        echo "Config '$yaml_config' not found"
        exit 1
    fi
    local var_names=$(cat $yaml_config | grep -v "\-\-\-" | yq -r '.|keys[]' | tr '\n' ' ')
    for var_name in $var_names; do
        var_value="$(cat $yaml_config | grep -v "\-\-\-" | yq -r ".$var_name")"
        # uppercase var from .yaml (require bash > 4.0)
        eval export "${var_name^^}=\"$var_value\""
    done
}

# Load .properties-style config and set bash variables from it
function load_jenkins_config()
{
    local yaml_config="$1"
    local var_name=
    local var_value=
    if [ "$yaml_config" = "" ]; then
        echo "Config name required!"
        exit 1
    fi
    if [ ! -s "$yaml_config" ]; then
        echo "Config '$yaml_config' not found"
        exit 1
    fi
    local var_names=$(cat $yaml_config | yq -r '.[].key' | tr '\n' ' ')
    for var_name in $var_names; do
        var_value="$(cat $yaml_config | yq -r ".[]|select(.key==\"$var_name\").value")"         # "
        # upper case variable name
        eval export ${var_name}="$var_value"
    done
}

