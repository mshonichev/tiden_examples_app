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
TAG=
FETCH_DEPENDENCIES=false
DIST_SOURCE=${DIST_SOURCE:-gridgain-ultimate-fabric}
CHECK_TEST_PARAMS=true

WITH_ZOOKEEPER=${WITH_ZOOKEEPER:-yes}
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-3.4.13}

WITH_MYSQL=${WITH_MYSQL:-no}
MYSQL_VERSION=${MYSQL_VERSION:-8.0.11}

WITH_HAZELCAST=${WITH_HAZELCAST:-no}
HAZELCAST_VERSION=${HAZELCAST_VERSION:-3.12}
WITH_TENSORFLOW=${WITH_TENSORFLOW:-no}
WITH_JFR=${WITH_JFR:-no}
WITH_FLAMEGRAPH=${WITH_FLAMEGRAPH:-no}

QA_FTP_HOST=${QA_FTP_HOST:-172.25.2.50}
QA_FTP_PATH=${QA_FTP_PATH:-/test-tools}
QA_FTP_CRED=${QA_FTP_CRED:-anonymous}

WITH_PICLIENT=${WITH_PICLIENT:-yes}

WITH_SBT_MODEL=${WITH_SBT_MODEL:-no}

check_download=1

function download_tool()
{
    local tool_name="$1"
    local tool_archive_name="$2"
    local base_ftp_path="${QA_FTP_PATH}"
    local tool_version=""
    shift
    shift
    while [ ! "$1" = "" ]; do
        # if next argument has '/' anywhere, consider it is a path, otherwise a tool version
        if [ ! "${1/\//}" = "$1" ]; then
            base_ftp_path="$1"
            shift
        else
            tool_version="$1"
            shift
        fi
    done

    if [ ! "$tool_version" = "" ]; then
        local s_ver=${tool_version//./\\.}
        tool_archive_name=$(echo "$tool_archive_name" | sed "s/[0-9]\+\(\\.[0-9]\+\)*/$s_ver/")
    fi

    local ftp_url="${QA_FTP_HOST}/${base_ftp_path}/${tool_archive_name}"
    ftp_url="ftp://${ftp_url//\/\//\/}"

    if [ "$tool_version" = "" ]; then
        echo "#### Download ${tool_name} ####"
    else
        echo "#### Download ${tool_name} version ${tool_version} ####"
    fi
    curl -u "$QA_FTP_CRED" -O "$ftp_url"
    res=$?
    if [ $res -ne 0 ]; then
        if [ $check_download -eq 1 ]; then
            if [ "$tool_version" = "" ]; then
                echo -e "\nERROR: Can't find $tool_name on QA FTP\nPlease, check url is valid: $ftp_url"
            else
                echo -e "\nERROR: Can't find $tool_name '$tool_version' on QA FTP\nPlease, check url is valid: $ftp_url"
            fi
            exit 2
        fi
    else
        echo -e "\nDownloaded $tool_archive_name successfully"
    fi
    return $res
}

function download_build()
{
    local build_archive_name="$1"
    local build_name="$2"
    local ignite_version="$3"
    local gridgain_version="${4:-$ignite_version}"

    download_tool "$build_name build" "$build_archive_name" "/$ignite_version/dev" "$gridgain_version"
}

function download_build_safe()
{
    # This is used to fallback from gridgain-ultimate-fabric to gridgain-ultimate if original download is failed

    check_download=0

    local build_archive_name="$1"
    local build_name="$2"
    local ignite_version="$3"
    local gridgain_version="${4:-$ignite_version}"
    local did_fallback=0

    download_tool "$build_name build" "$build_archive_name" "/$ignite_version/dev" "$gridgain_version"
    if [ $? -ne 0 ]; then
        alt_build_archive_name=$(echo $build_archive_name | sed 's/-fabric//')
        did_fallback=1
        download_tool "$build_name build" "$alt_build_archive_name" "/$ignite_version/dev" "$gridgain_version"
        if [ $? -ne 0 ]; then
            echo -e "\nERROR: Can't find neither $alt_build_archive_name nor $build_archive_name on QA FTP"
            exit 2
        fi
    fi
    check_download=1

    return $did_fallback
}

function print_jenkins_build_description()
{
    description=

    if [ "$DIST_SOURCE" = "" ]; then
        echo "DIST_SOURCE must be set"
        exit 1
    elif [ "$DIST_SOURCE" = "gridgain-ultimate-fabric" -o "$DIST_SOURCE" = "gridgain-ultimate" ]; then
        if [ "$GRIDGAIN_VERSION" = "" ]; then
            echo "GRIDGAIN_VERSION must be set"
            exit 1
        fi
        if [ "$IGNITE_VERSION" = "" ]; then
            echo "IGNITE_VERSION must be set"
            exit 1
        fi
        description="GG UE $GRIDGAIN_VERSION"
    elif [ "$DIST_SOURCE" = "apache-ignite" -o "$DIST_SOURCE" = "apache-ignite-fabric" ]; then
        if [ "$IGNITE_VERSION" = "" ]; then
            echo "IGNITE_VERSION must be set"
            exit 1
        fi
        description="AI PE $IGNITE_VERSION"
    else
        echo "Unknown DIST_SOURCE '$DIST_SOURCE', choose 'gridgain-ultimate' or 'apache-ignite'"
        exit 1
    fi

    if [ ! "$PREV_IGNITE_VERSION" = "" ]; then
        OFS=$IFS
        IFS=','
        PREV_IGNITE_VERSION_LIST="$PREV_IGNITE_VERSION"
        first_compat=1
        for PREV_IGNITE_VERSION in $PREV_IGNITE_VERSION_LIST; do
            if [ "$PREV_IGNITE_VERSION" = "$IGNITE_VERSION" ]; then
                continue
            fi

            if [ "$DIST_SOURCE" = "gridgain-ultimate-fabric" -o "$DIST_SOURCE" = "gridgain-ultimate" ]; then
                # calculate GG version as IGN version + 6 if ignite_version less than 8
                shift=0
                if [[ "$(echo $PREV_IGNITE_VERSION | cut -d'.' -f 1)" -lt 8 ]]
                then
                  shift=6
                fi
                maj_gg_version=$(echo $(echo $PREV_IGNITE_VERSION | cut -d'.' -f 1) + $shift | bc)
                PREV_GRIDGAIN_VERSION="$maj_gg_version.$(echo $PREV_IGNITE_VERSION | cut -d'.' -f 2-)"
                if [ $first_compat -eq 1 ]; then
                    description="$description vs $PREV_GRIDGAIN_VERSION"
                else
                    description="$description,$PREV_GRIDGAIN_VERSION"
                fi
            elif [ "$DIST_SOURCE" = "apache-ignite" -o "$DIST_SOURCE" = "apache-ignite-fabric" ]; then
                if [ $first_compat -eq 1 ]; then
                    description="$description vs $PREV_IGNITE_VERSION"
                else
                    description="$description,$PREV_IGNITE_VERSION"
                fi
            fi
            first_compat=0
        done
        PREV_IGNITE_VERSION="$PREV_IGNITE_VERSION_LIST"
        IFS=$OFS
    fi


    echo "Desc: $description"
}

function fetch_source_archives_to_work_dir()
{
    pushd $WORK_DIR >/dev/null

    if [ "$QA_FTP_HOST" = "" -o "$QA_FTP_CRED" = "" ]; then
        echo "QA_FTP_HOST/QA_FTP_CRED not set, do not downloading sources"
    else
        if [ "$DIST_SOURCE" = "gridgain-ultimate-fabric" -o "$DIST_SOURCE" = "gridgain-ultimate" ]; then
            download_build_safe "$DIST_SOURCE-0.0.0.zip" "GG UE" $IGNITE_VERSION "$GRIDGAIN_VERSION"
            if [ $? -eq 1 ]; then
                if [ "$DIST_SOURCE" = "gridgain-ultimate-fabric" ]; then
                    DIST_SOURCE=gridgain-ultimate
                elif [ "$DIST_SOURCE" = "gridgain-ultimate" ]; then
                    DIST_SOURCE=gridgain-ultimate-fabric
                fi
            fi
        elif [ "$DIST_SOURCE" = "apache-ignite" -o "$DIST_SOURCE" = "apache-ignite-fabric" ]; then
            download_build "$DIST_SOURCE-0.0.0-bin.zip" "AI PE" $IGNITE_VERSION
            if [ $? -eq 1 ]; then
                if [ "$DIST_SOURCE" = "apache-ignite-fabric" ]; then
                    DIST_SOURCE=apache-ignite
                elif [ "$DIST_SOURCE" = "apache-ignite" ]; then
                    DIST_SOURCE=apache-ignite-fabric
                fi
            fi
        fi

        if [ ! "$PREV_IGNITE_VERSION" = "" ]; then
            OFS=$IFS
            IFS=','
            PREV_IGNITE_VERSION_LIST="$PREV_IGNITE_VERSION"
            for PREV_IGNITE_VERSION in $PREV_IGNITE_VERSION_LIST; do
                if [ "$PREV_IGNITE_VERSION" = "$IGNITE_VERSION" ]; then
                    continue
                fi

                if [ "$DIST_SOURCE" = "gridgain-ultimate-fabric" -o "$DIST_SOURCE" = "gridgain-ultimate" ]; then
                    # calculate GG version as IGN version + 6 if ignite_version less than 8
                    shift=0
                    if [[ "$(echo $PREV_IGNITE_VERSION | cut -d'.' -f 1)" -lt 8 ]]
                    then
                      shift=6
                    fi
                    maj_gg_version=$(echo $(echo $PREV_IGNITE_VERSION | cut -d'.' -f 1) + $shift | bc)
                    PREV_GRIDGAIN_VERSION="$maj_gg_version.$(echo $PREV_IGNITE_VERSION | cut -d'.' -f 2-)"
                    download_build_safe "gridgain-ultimate-fabric-0.0.0.zip" "GG UE" "$PREV_IGNITE_VERSION" "$PREV_GRIDGAIN_VERSION"
                elif [ "$DIST_SOURCE" = "apache-ignite" -o "$DIST_SOURCE" = "apache-ignite-fabric" ]; then
                    download_build_safe "apache-ignite-fabric-0.0.0-bin.zip" "AI PE" "$PREV_IGNITE_VERSION"
                fi

            done
            IFS=$OFS
            PREV_IGNITE_VERSION="$PREV_IGNITE_VERSION_LIST"
        fi

        download_tool "Test Tools (SNAPSHOT)" ignite-test-tools-1.0.0-SNAPSHOT.jar
#        download_tool "Test Tools (INDEX)" ignite-test-tools-1.0.0-INDEX.jar

        if [ ! "$WITH_SBT_MODEL" = "no" ]; then
            if [ "$WITH_SBT_MODEL" = "yes" ]; then
                sbt_model_variant='sbt_model.zip'
            else
                sbt_model_variant="$WITH_SBT_MODEL"
            fi

            download_tool "Sberbank caches model $(echo $sbt_model_variant|cut -d'.' -f 1)" $sbt_model_variant
        fi

        if [ ! "$WITH_PICLIENT" = "no" ]; then
            if [ "$WITH_PICLIENT" = "yes" ]; then
                pi_client_version='2.7.2'
            else
                pi_client_version="$WITH_PICLIENT"
            fi
            download_tool "PI Client $pi_client_version" piclient-$pi_client_version.jar
        fi

        if [ "$WITH_ZOOKEEPER" = "yes" ]; then
            download_tool "Zookeeper" zookeeper-${ZOOKEEPER_VERSION}.tar.gz /zookeeper
        fi

        if [ "$WITH_FLAMEGRAPH" = "yes" ]; then
            download_tool "Flamegraph libs" async_flamegraph.zip /benchmarks/tools
        fi

        # JFR profiles are placed under CVS
        #if [ "$WITH_JFR" = "yes" ]; then
        #    download_tool "GridGain JFR profile" jfr.zip /benchmarks/tools
        #fi

        if [ "$WITH_MYSQL" = "yes" ]; then
            download_tool "MySQL" mysql-${MYSQL_VERSION}-linux-glibc2.12-x86_64.tar.gz /tools
        fi

        if [ "$WITH_HAZELCAST" = "yes" ]; then
            download_tool "Hazelcast" hazelcast-${HAZELCAST_VERSION}.zip /tools
        fi

        if [ "$WITH_TENSORFLOW" = "yes" ]; then
            echo "#### Download Tensorflow artifacts ####"
            curl -u "$QA_FTP_CRED" -O "ftp://172.25.2.50/ml/cache-initializer.jar"
            curl -u "$QA_FTP_CRED" -O "ftp://172.25.2.50/ml/ignite-tf.tar"
            curl -u "$QA_FTP_CRED" -O "ftp://172.25.2.50/ml/tf-executor.tar"
            if [ $? -ne 0 ]; then
                echo "ERROR: Can't find Tensorflow artifacts distrib on QA FTP"
                exit 2
            fi
        fi
    fi

    if [ "$GRIDGAIN_VERSION" = "" ]; then
        if [ -f $DIST_SOURCE-$IGNITE_VERSION-bin.zip ]; then
            unzip $DIST_SOURCE-$IGNITE_VERSION-bin.zip \
                    $DIST_SOURCE-$IGNITE_VERSION-bin/libs/ignite-core-*.jar -d tmp
            unzip -q -o tmp/$DIST_SOURCE-$IGNITE_VERSION-bin/libs/ignite-core-*.jar ignite.properties
            rm -rf tmp
        else
            echo "ERROR: can't extract 'ignite.properties'"
        fi
    else
        if [ -f $DIST_SOURCE-$GRIDGAIN_VERSION.zip ]; then
            unzip $DIST_SOURCE-$GRIDGAIN_VERSION.zip \
                    $DIST_SOURCE-$GRIDGAIN_VERSION/libs/ignite-core-*.jar -d tmp
            unzip $DIST_SOURCE-$GRIDGAIN_VERSION.zip \
                    $DIST_SOURCE-$GRIDGAIN_VERSION/libs/gridgain-core-*.jar -d tmp
            unzip -q -o tmp/$DIST_SOURCE-$GRIDGAIN_VERSION/libs/ignite-core-*.jar ignite.properties
            unzip -q -o tmp/$DIST_SOURCE-$GRIDGAIN_VERSION/libs/gridgain-core-*.jar gridgain.properties
            rm -rf tmp
        else
            echo "ERROR: can't extract 'ignite.properties' and 'gridgain.properties'"
            exit 3
        fi
    fi

    popd >/dev/null
}


# ====================================================================================================================
# MAIN: parse options, prepare work directory
# ====================================================================================================================

#load_defaults

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
    if [ "$opt" = '--no-check' ]; then
        CHECK_TEST_PARAMS=false
        continue
    fi
    if [ "$opt" = "--fetch-deps" ]; then
        FETCH_DEPENDENCIES=true
        continue
    fi
    if [ "$opt" = '--tag' ]; then
        TAG="$val"
        continue
    fi
    echo "Unknown option: $opt"
    exit 1
done

if [ "$WORK_DIR" = "" ]; then
    echo "WORK_DIR must be set with --work-dir option"
    exit 1
fi
echo "WORK_DIR: $WORK_DIR"

mkdir -p $WORK_DIR

if [ "$VAR_DIR" = "" ]; then
    echo "VAR_DIR must be set with --var-dir option"
    exit 1
fi
echo "VAR_DIR: $VAR_DIR"

mkdir -p $VAR_DIR

set | grep -E "^(WITH_|DIST)"

print_jenkins_build_description
fetch_source_archives_to_work_dir
