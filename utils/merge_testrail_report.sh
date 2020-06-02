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


. $(dirname $0)/functions.inc.sh

WORK_DIR=${WORK_DIR:-}
VAR_DIR=${VAR_DIR:-}

QA_FTP_HOST=${QA_FTP_HOST:-172.25.2.50}
QA_FTP_PATH=${QA_FTP_PATH:-/}
QA_FTP_CRED=${QA_FTP_CRED:-anonymous}

REPORTS=

function download_report()
{
    local report_name="$1"
    local ignite_version="$2"
    local output_directory="${3:-.}"

    local base_ftp_path="${QA_FTP_PATH}/${ignite_version}/report/"
    local ftp_url="${QA_FTP_HOST}/${base_ftp_path}/${report_name}"

    ftp_url="ftp://${ftp_url//\/\//\/}"

    echo "#### Download previous '${report_name}' for '${ignite_version}' ####"

    curl -u "$QA_FTP_CRED" -o "${output_directory}/${report_name}" "$ftp_url"
    res=$?
    return $res
}

function merge_report()
{
    local report_name="$(basename  $1)"
    local report_path="$1"
    shift

    download_report "$report_name" "$IGNITE_VERSION" "$DOWNLOAD_DIR"
    if [ $? -ne 0 ]; then
        echo "INFO: no report '$report_name' found at QA FTP, nothing to merge with"
        return
    fi
    if [ ! -s $DOWNLOAD_DIR/$report_name ]; then
        echo "WARN: empty report '$report_name' found at QA FTP, removing"
        rm -f $DOWNLOAD_DIR/$report_name
        return
    fi

    python3 -W ignore utils/merge_testrail_reports.py "$DOWNLOAD_DIR/$report_name" "$report_path" "$OUTPUT_DIR/$report_name"

    if [ -s $OUTPUT_DIR/$report_name ]; then
        echo "INFO: Updating $report_path"
        mv $report_path $report_path.bak
        mv -f $OUTPUT_DIR/$report_name $report_path
        return
    fi
}

# ====================================================================================================================
# MAIN: parse options, prepare work directory
# ====================================================================================================================

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

    if [ "$opt" = "--work-dir" -o "$opt" = "--work_dir" ]; then
        WORK_DIR="$val"
        continue
    fi
    if [ "$opt" = "--var-dir" -o "$opt" = "--var_dir" ]; then
        VAR_DIR="$val"
        continue
    fi
    if [ "$opt" = '--jenkins-config' ]; then
        load_jenkins_config "$val"
        continue
    fi
    if [ "$opt" = '--config' ]; then
        load_config "$val"
        continue
    fi
    if [ "$opt" = '--report' ]; then
        for report in $val; do
            if [ ! -f "$report" ]; then
                echo "WARN: report '$report' not found, skipping"
                continue
            fi
            if [ ! -s "$report" ]; then
                echo "WARN: report '$report' is empty, skipping"
                continue
            fi
            if [ "$REPORTS" = "" ]; then
                REPORTS="$report"
            else
                REPORTS="$REPORTS $report"
            fi
        done
        continue
    fi

    echo "ERROR: Unknown option: $opt"
    exit 1
done

if [ "$REPORTS" = "" ]; then
    echo "INFO: Nothing to do"
    exit 0
fi

#if [ "$WORK_DIR" = "" ]; then
#    echo "WORK_DIR must be set with --work-dir option"
#    exit 1
#fi
#echo "WORK_DIR: $WORK_DIR"
#
#mkdir -p $WORK_DIR

if [ "$VAR_DIR" = "" ]; then
    echo "ERROR: VAR_DIR must be set with --var-dir option"
    exit 1
fi
echo "  VAR_DIR: $VAR_DIR"

mkdir -p $VAR_DIR

DOWNLOAD_DIR=$VAR_DIR/merge_tmp/prev
OUTPUT_DIR=$VAR_DIR/merge_tmp/merge
mkdir -p $DOWNLOAD_DIR
mkdir -p $OUTPUT_DIR

if [ "$IGNITE_VERSION" = "" ]; then
    echo "IGNITE_VERSION must be set"
    exit 2
fi
echo "  IGNITE_VERSION: $IGNITE_VERSION"

OFS=$IFS
IFS=' '
for report in $REPORTS; do
    if [ "$report" = "" ]; then continue; fi
    echo "INFO: Processing '$report'"
    merge_report "$report"
done
IFS=$OFS

rm -rf $VAR_DIR/merge_tmp
