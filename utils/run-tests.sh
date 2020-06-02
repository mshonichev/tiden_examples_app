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
# Run Tiden tests shell wrapper. Parses well-known environment variables
# to Tiden framework startup options.
#
# Usage:
#   bash \
#       utils/run-tests.sh \
#           <suite_name> [<suite_pattern>]
#
# Where:
#   suite_name - short name of suite under test, will be used in report names
#   suite_pattern - Tiden suite pattern, e.g. `--ts` value
#
# Expects to see following variables in the environment:
#
#   SHARED_ROOT - root directory for framework, equals to "." if not set
#
#   ATTR - Tiden attributes, e.g. `--attr` value, a list split by ','
#   OPTION - Tiden options, e.g. `--to` value, a list split by ' '
#   SUITE_CONFIG - Tiden configs, e.g. `--tc` value, a list split by ','
#       when not set, default is determined via DIST_SOURCE
#   CLEAN - Tiden clean option, e.g. `--clean` value, default `--clean=all`
#
#   DIST_SOURCE - defaults to "gridgain-ultimate-fabric"
#       determines default value for artifacts config, either
#       `jenkins-artifacts-ai.yaml` or `jenkins-artifacts-gg-ult-fab.yaml`
#
#   ATTR_MATCH - Tiden attr_match option value, default `--to=attr_match=any`
#
#   SERVER_HOSTS - passed as is to `--to=environment.server_hosts`
#   CLIENT_HOSTS - passed as is to `--to=environment.client_hosts`
#   ZK_HOSTS - passed as is to `--to=environment.zookeeper_hosts`
#
##############################################################################


# short name of the suite under test,
SUITE_NAME="$1"

# --ts= pattern for running suite. defaults to suite name.
SUITE_PATTERN="${2:-$SUITE_NAME}"

SHARED_ROOT="${SHARED_ROOT:-.}"

# ROOT_DIR_NAME is set by Jenkins to 'tiden-dev'
# BRANCH is set by Jenkins to e.g. 'origin/master'
# SHARED_ROOT is local directory set by Jenkins to /var/jenkins_home/workspace/$ROOT_DIR_NAME/$BRANCH
# JOB_NAME is set by Jenkins to smth. like 'tiden/suite-selfcheck'
# ATTR is set to '' or a comma separated list of attributes

REMOTE_DIR_NAME=$ROOT_DIR_NAME/$(echo $JOB_NAME | cut -d '/' -f 2-)/$BRANCH

DIST_SOURCE=${DIST_SOURCE:-gridgain-ultimate-fabric}

echo "*** Remote home: /storage/ssd/prtagent/$REMOTE_DIR_NAME ***"


DESCRIPTION=

ATTR_LIST=
if [ "$ATTR" = "" ]; then
    DESCRIPTION="all suite '$SUITE_PATTERN'"
else
    OFS=$IFS
    IFS=','
    for attr_val in $ATTR; do
        if [ "$ATTR_LIST" = "" ]; then
            ATTR_LIST="--attr=$attr_val"
        else
            ATTR_LIST="$ATTR_LIST --attr=$attr_val"
        fi
    done
    IFS=$OFS

    DESCRIPTION="all tests in '$SUITE_PATTERN' with attrs '$ATTR_LIST'"
fi

OPTIONS_LIST=
if [ ! "$OPTION" = "" ]; then
    OFS=$IFS
    IFS=' '
    for option_val in $OPTION; do
        if [ "$option_val" = "" ]; then continue; fi
        DESCRIPTION="$DESCRIPTION, $option_val"
        if [ "$OPTIONS_LIST" = "" ]; then
            OPTIONS_LIST="--to=$option_val"
        else
            OPTIONS_LIST="$OPTIONS_LIST --to=$option_val"
        fi
    done
    IFS=$OFS
fi

CONFIG_LIST=
if [ "$SUITE_CONFIG" = "" ]; then
    # default artifacts config is plain
    case $DIST_SOURCE in
        gridgain-ultimate*)
            SUITE_CONFIG="env_jenkins.yaml,jenkins-artifacts-gg-ult-fab.yaml"
            ;;
        apache-ignite*)
            SUITE_CONFIG="env_jenkins.yaml,jenkins-artifacts-ai.yaml"
            ;;
    esac
fi

OFS=$IFS
IFS=','
for config_name in $SUITE_CONFIG; do
    if [ "$CONFIG_LIST" = "" ]; then
        CONFIG_LIST="--tc=$SHARED_ROOT/work/$config_name"
    else
        CONFIG_LIST="$CONFIG_LIST --tc=$SHARED_ROOT/work/$config_name"
    fi
done
IFS=$OFS

echo "Desc: $DESCRIPTION"

if [ "$CLEAN" = "all" -o "$CLEAN" = "" ]; then
    CLEAN_OPTION="--clean=all"
elif [ "$CLEAN" = "none" ]; then
    CLEAN_OPTION=""
elif [ "$CLEAN" = "tests" ]; then
    CLEAN_OPTION="--clean=tests"
fi

PREV_IGNITE_VERSION=${PREV_IGNITE_VERSION:-}
IGNITE_VERSION=${IGNITE_VERSION:-}
DIST_SOURCE=${DIST_SOURCE:-gridgain-ultimate-fabric}

# for compatibility tests, use IGNITE_VERSION and PREV_IGNITE_VERSION to generate report name,
# e.g.
#   report-suite-snapshots_zk-2.1.4.yaml
#
if [ "$PREV_IGNITE_VERSION" = "" -o "$PREV_IGNITE_VERSION" = "$IGNITE_VERSION" ]; then
    report_suffix=''
else
    report_suffix=''
    OFS=$IFS
    IFS=','
    for prev_version in $PREV_IGNITE_VERSION; do
        if [ "$prev_version" = "$IGNITE_VERSION" ]; then
            continue
        fi
        if [ "$DIST_SOURCE" = "gridgain-ultimate" -o "$DIST_SOURCE" = "gridgain-ultimate-fabric" ]; then
            shift=0
            if [[ "$(echo $prev_version | cut -d'.' -f 1)" -lt 8 ]]
            then
              shift=6
            fi
            maj_gg_version=$(echo $(echo $prev_version | cut -d'.' -f 1) + $shift | bc)
            prev_version="$maj_gg_version.$(echo $prev_version | cut -d'.' -f 2-)"
        fi
        report_suffix="$report_suffix-$prev_version"
    done
    IFS=$OFS
fi

set -x
set -e

python3 -W ignore \
	run-tests.py \
    	--var_dir=$SHARED_ROOT/var \
    	${CONFIG_LIST} \
        --to=attr_match=${ATTR_MATCH} ${ATTR_LIST} ${OPTIONS_LIST} \
		--to=environment.server_hosts=$SERVER_HOSTS \
		--to=environment.client_hosts=$CLIENT_HOSTS \
		--to=environment.zookeeper_hosts=$ZK_HOSTS \
		--to=environment.home=/storage/ssd/prtagent/$REMOTE_DIR_NAME \
		--to=environment.shared_home=/mnt/lab_share01/prtagent/$REMOTE_DIR_NAME \
		--to=testrail_report=report-suite-${SUITE_NAME}${report_suffix}.yaml \
		--to=xunit_file=xunit-${SUITE_NAME}${report_suffix}.xml \
        --ts="$SUITE_PATTERN" ${CLEAN_OPTION}

res=$?

if [[ $res!=0 ]]; then
    exit $res;
fi

if [ "$MERGE_REPORTS" = "true" ]; then
    bash \
        utils/merge_testrail_report.sh \
            --var-dir=$SHARED_ROOT/var \
            --config=$WORKSPACE/qa_ftp.yaml \
            --report=$SHARED_ROOT/var/report-suite-*.yaml
fi
