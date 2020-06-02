String DFLT_CFG_PERSISTENCE_ENABLED = "false"
String DFLT_CFG_SYNC_MODE = "PRIMARY_SYNC"
String DFLT_CFG_WAL_MODE = "LOG_ONLY"
String DFLT_CFG_CACHE_MODE = "PARTITIONED"
String DFLT_SSL_ENABLED = "false"

String CHILD_JOB = "yardstick-benchmarks"
// String CHILD_JOB = "/Benchmarks/yardstick-benchmarks"

properties([
    buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '90', numToKeepStr: '')),
    parameters([
        string(defaultValue: '', description: '''GG PE/CE or Ignite version, GG branch, path to ZIP on QA FTP. Examples:
2.7.0 -- AI 2.7;
2.5.3 -- GG PE 2.5.3;
8.7.3 -- GG CE 8.7.3;
8.7-master -- latest CE build from 8.7-master on private TeamCity;
jenkins/benchmarks/test.zip -- relative path to artifact on QA FTP.''', name: 'BASE_IGNITE_VERSION'),
        string(defaultValue: '',
            description: 'Same as for BASE_IGNITE_VERSION, but more than one test can be specified (comma-separated).',
            name: 'TEST_IGNITE_VERSION'),
        [$class: 'ParameterSeparatorDefinition', name: 'separator-36e2e705-5e54-46e6-b8e5-b1c325ee9535', sectionHeader: '', sectionHeaderStyle: '', separatorStyle: ''],
        string(defaultValue: 'origin/master', description: '', name: 'TIDEN_BRANCH'),
        string(defaultValue: '', description: 'Comma-separated IP addresses', name: 'SERVER_HOSTS'),
        string(defaultValue: '', description: 'Comma-separated IP addresses', name: 'CLIENT_HOSTS'),

        [$class: 'ChoiceParameter', choiceType: 'PT_CHECKBOX', description: 'Test method attributes', filterLength: 1, filterable: true, name: 'ATTRIB', randomName: 'choice-parameter-16307342944773775', script: [$class: 'GroovyScript', fallbackScript: [classpath: [], sandbox: false, script: ''], script: [classpath: [], sandbox: false, script: '''return [
\'release_smoke\',
\'release_full\',
\'all_cache\',
\'all_atomic\',
\'all_tx\',
\'all_compute\',
\'all_sql_query\',
\'all_sql_jdbc\',
\'test_ignite_atomic_put\',
\'test_ignite_atomic_put_random_value\',
\'test_ignite_atomic_get\',
\'test_ignite_atomic_put_all\',
\'test_ignite_atomic_get_all\',
\'test_ignite_atomic_put_get\',
\'test_ignite_atomic_page_replacement\',
\'test_ignite_tx_invoke\',
\'test_ignite_tx_put\',
\'test_ignite_tx_opt_put\',
\'test_ignite_tx_opt_serial_put\',
\'test_ignite_tx_put_all\',
\'test_ignite_tx_put_get\',
\'test_ignite_tx_get\',
\'test_ignite_tx_get_all\',
\'test_ignite_tx_opt_rep_read_put_get\',
\'test_ignite_tx_pess_rep_read_put_get\',
\'test_ignite_tx_opt_serial_put_get\',
\'test_ignite_sql_query\',
\'test_ignite_sql_query_join\',
\'test_ignite_sql_query_put\',
\'test_ignite_tx_get_all_put_all_bytes_key\',
\'test_ignite_atomic_get_and_put_bytes_key\',
\'test_ignite_tx_get_and_put_bytes_key\',
\'test_ignite_atomic_put_bytes_key\',
\'test_ignite_sql_query_hash_join_hj\',
\'test_ignite_sql_query_hash_join_NL_idx_fx_d\',
\'test_ignite_sql_query_hash_join_nl_idx_d_fx\',
\'test_affcall_compute\',
\'test_apply_compute\',
\'test_broad_compute\',
\'test_exec_compute\',
\'test_run_compute\',
\'test_ignite_sql_merge\',
\'test_ignite_sql_merge_query\',
\'test_ignite_sql_merge_indexed1\',
\'test_ignite_sql_merge_indexed2\',
\'test_ignite_sql_merge_indexed8\',
\'test_ignite_sql_insert_indexed1\',
\'test_ignite_sql_insert_indexed2\',
\'test_ignite_sql_insert_indexed8\',
\'test_ignite_sql_delete_filtered\',
\'test_ignite_sql_update\',
\'test_ignite_sql_update_filtered\',
\'test_ignite_sql_mixed_date_inline_r1\',
\'test_ignite_sql_insert_delete_native_r1\',
\'test_ignite_sql_insert_delete_jdbc_thin_r1\',
\'test_ignite_sql_update_native_r1\',
\'test_ignite_sql_update_jdbc_thin_r1\',
\'test_ignite_sql_update_native_r1000\',
\'test_ignite_sql_update_jdbc_thin_r1000\',
\'test_ignite_sql_limit\',
\'test_ignite_sql_sort_idx\',
\'test_ignite_sql_distinct\',
\'test_ignite_sql_group_idx\',
\'test_ignite_sql_group_distinct\',
\'test_ignite_sql_group_non_idx\'
]''']]],

        string(defaultValue: '', description: 'A list of additional test names, not present in the ATTRIB list (comma-separated)', name: 'ATTRIB_CUSTOM'),
        choice(choices: ['any', 'all'], description: 'Logical operation used to select test methods by their attributes. \'any\' means \'OR\', \'all\' means \'AND\'', name: 'ATTR_MATCH'),
        string(defaultValue: '', description: 'Additional parameters for the test runner', name: 'ADD_PARAMS'),
        string(defaultValue: '', description: 'Options to pass as --to parameter (space-separated). E.g.: backups=2 threads=1', name: 'TEST_OPTIONS'),

        string(defaultValue: '60', description: 'Benchmark warmup time (seconds)', name: 'CFG_WARMUP'),
        string(defaultValue: '60', description: 'Benchmark duration (seconds)', name: 'CFG_DURATION'),
        string(defaultValue: '3', description: 'Maximum number of benchmark retries when performance differs significantly', name: 'CFG_MAX_RETRY_ATTEMPTS'),

        [$class: 'ChoiceParameter', choiceType: 'PT_CHECKBOX', description: 'Default: ' + DFLT_CFG_PERSISTENCE_ENABLED, filterLength: 1, filterable: false, name: 'CFG_PERSISTENCE_ENABLED', randomName: 'choice-parameter-16307355345772765', script: [$class: 'GroovyScript', fallbackScript: [classpath: [], sandbox: false, script: ''], script: [classpath: [], sandbox: false, script: '''return [
\'true\',
\'false\'
]''']]],
        [$class: 'ChoiceParameter', choiceType: 'PT_CHECKBOX', description: 'Default: ' + DFLT_CFG_SYNC_MODE, filterLength: 1, filterable: false, name: 'CFG_SYNC_MODE', randomName: 'choice-parameter-16307352345542775', script: [$class: 'GroovyScript', fallbackScript: [classpath: [], sandbox: false, script: ''], script: [classpath: [], sandbox: false, script: '''return [
\'PRIMARY_SYNC\',
\'FULL_SYNC\'
]''']]],
        [$class: 'ChoiceParameter', choiceType: 'PT_CHECKBOX', description: 'Default: ' + DFLT_CFG_CACHE_MODE, filterLength: 1, filterable: false, name: 'CFG_CACHE_MODE', randomName: 'choice-parameter-16307352345533775', script: [$class: 'GroovyScript', fallbackScript: [classpath: [], sandbox: false, script: ''], script: [classpath: [], sandbox: false, script: '''return [
\'PARTITIONED\',
\'REPLICATED\'
]''']]],
        [$class: 'ChoiceParameter', choiceType: 'PT_CHECKBOX', description: 'WAL mode. Effective only when persistence is enabled. Default: ' + DFLT_CFG_WAL_MODE, filterLength: 1, filterable: false, name: 'CFG_WAL_MODE', randomName: 'choice-parameter-16325234555234575', script: [$class: 'GroovyScript', fallbackScript: [classpath: [], sandbox: false, script: ''], script: [classpath: [], sandbox: false, script: '''return [
\'LOG_ONLY\',
\'FSYNC\',
\'BACKGROUND\',
\'NONE\'
]''']]],
        [$class: 'ChoiceParameter', choiceType: 'PT_CHECKBOX', description: 'Default: ' + DFLT_SSL_ENABLED, filterLength: 1, filterable: false, name: 'SSL_ENABLED', randomName: 'choice-parameter-16307355348772765', script: [$class: 'GroovyScript', fallbackScript: [classpath: [], sandbox: false, script: ''], script: [classpath: [], sandbox: false, script: '''return [
\'true\',
\'false\'
]''']]],

        booleanParam(name: 'RECORD_JFR', defaultValue: false, description: 'Run with Java Flight Recorder'),
        booleanParam(name: 'RECORD_FLAMEGRAPH', defaultValue: false, description: 'Generate Flamegraphs with async-profiler'),
        booleanParam(name: 'UPLOAD_TO_ECO_SYSTEM', defaultValue: true, description: ''),
        string(defaultValue: '', description: 'Eco-system comment', name: 'ECO_SYSTEM_COMMENT'),
        booleanParam(name: 'UPLOAD_REPORT_TO_QA_FTP', defaultValue: false, description: 'Upload report to QA FTP. Target directory: <test-1-ignite-version>/report/')
    ])
])

node {
    stage('Prepare') {
        // Check job parameters
        if (params.SERVER_HOSTS.isEmpty()) error "Please set SERVER_HOSTS"
        if (params.CLIENT_HOSTS.isEmpty()) error "Please set CLIENT_HOSTS"
        if (params.BASE_IGNITE_VERSION.isEmpty()) error "Please set BASE_IGNITE_VERSION"
        if (params.CFG_WARMUP.isEmpty()) error "Please set CFG_WARMUP"
        if (params.CFG_DURATION.isEmpty()) error "Please set CFG_DURATION"
        if (params.CFG_MAX_RETRY_ATTEMPTS.isEmpty()) error "Please set CFG_MAX_RETRY_ATTEMPTS"

        // Set build name
        def displayVersion = params.TEST_IGNITE_VERSION.isEmpty() ? params.BASE_IGNITE_VERSION : params.TEST_IGNITE_VERSION
        currentBuild.displayName = currentBuild.displayName + '-' + displayVersion

        // Delete workspace before build starts
        deleteDir()

        // Checkout Tiden
//         checkout([$class: 'GitSCM', branches: [[name: params.TIDEN_BRANCH]],userRemoteConfigs: [[credentialsId: '0cc82f1a-e7dc-4db2-9774-7adfbd238b9b', url: 'https://github.com/ggprivate/tiden.git']]])
        checkout poll: false, scm: [$class: 'GitSCM', branches: [[name: params.TIDEN_BRANCH]], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'CleanBeforeCheckout'], [$class: 'CloneOption', noTags: true, reference: '', shallow: true]], submoduleCfg: [], userRemoteConfigs: [[credentialsId: '0cc82f1a-e7dc-4db2-9774-7adfbd238b9b', url: 'https://github.com/ggprivate/tiden.git']]]

        fileOperations([folderCreateOperation('output')])

        // Download artifacts
        // QA FTP credentials mask GGTC URLs
//         withCredentials([usernamePassword(credentialsId: 'QA_FTP', usernameVariable: 'QA_FTP_USER', passwordVariable: 'QA_FTP_PASSWD')]) {
        withEnv(['QA_FTP_USER=gg', 'QA_FTP_PASSWD=gg']) {
        withCredentials([usernamePassword(credentialsId: 'ggtc_creds', usernameVariable: 'GGTC_USER', passwordVariable: 'GGTC_PASSWD')]) {
            dir('output') {
                // Download BASE artifact
                echo "Downloading ${params.BASE_IGNITE_VERSION}..."
                sh '''#!/usr/bin/env bash
                python3 $WORKSPACE/utils/get_gridgain_artifacts.py \\
                    --search=$BASE_IGNITE_VERSION \\
                    --type=community \\
                    --tc_login="$GGTC_USER" \\
                    --tc_password="$GGTC_PASSWD" \\
                    --ftp_login="$QA_FTP_USER" \\
                    --ftp_password="$QA_FTP_PASSWD" \\
                    --prefix=base
                '''

                // Download TEST artifact(s)
                if (!params.TEST_IGNITE_VERSION.isEmpty()) {
                    params.TEST_IGNITE_VERSION.split(",").eachWithIndex { ver, i ->
                        echo "Downloading ${ver}..."
                        sh '''#!/usr/bin/env bash
                        python3 $WORKSPACE/utils/get_gridgain_artifacts.py \\
                            --search=''' + ver + ''' \\
                            --type=community \\
                            --tc_login="$GGTC_USER" \\
                            --tc_password="$GGTC_PASSWD" \\
                            --ftp_login="$QA_FTP_USER" \\
                            --ftp_password="$QA_FTP_PASSWD" \\
                            --prefix=test-''' + (i + 1)
                    }
                }

                // Upload artifacts to QA FTP
                sh '''#!/bin/bash
                set -e

                echo "Test artifacts:"
                ls -lA

                # Extract AI version of the first test artifact
                if [ -e test-1-*.zip ]; then
                    unzip -qjo test-1-\\*.zip \\*/libs/ignite-core\\*.jar && \\
                        unzip -p ignite-core\\*.jar ignite.properties | grep ignite.version > \\
                        test-1-ignite.properties
                    rm -rf ignite-core*.jar
                fi

                QA_FTP_ADDRESS="172.25.2.50"
                QA_FTP_ROOT="ftp://${QA_FTP_ADDRESS}"
                URI="$QA_FTP_ROOT/jenkins/benchmarks/$BUILD_NUMBER/"
                echo "Uploading Ignite/GridGain artifacts to $URI..."
                for file in *; do
                    curl -sS -T $file -u "$QA_FTP_USER:$QA_FTP_PASSWD" --ftp-create-dirs "$URI"
                done
                '''

            }
        }
        }
    }

    ////////////////////
    stage('Test') {
        // Set defaults
        String wal_mode = params.CFG_WAL_MODE.isEmpty() ? DFLT_CFG_WAL_MODE : params.CFG_WAL_MODE
        String sync_mode = params.CFG_SYNC_MODE.isEmpty() ? DFLT_CFG_SYNC_MODE : params.CFG_SYNC_MODE
        String cache_mode = params.CFG_CACHE_MODE.isEmpty() ? DFLT_CFG_CACHE_MODE : params.CFG_CACHE_MODE
        String pds_on_off = params.CFG_PERSISTENCE_ENABLED.isEmpty() ? DFLT_CFG_PERSISTENCE_ENABLED : params.CFG_PERSISTENCE_ENABLED
        String ssl_on_off = params.SSL_ENABLED.isEmpty() ? DFLT_SSL_ENABLED : params.SSL_ENABLED

        // Remove accidental spaces from hosts lists
        String server_hosts = params.SERVER_HOSTS.replaceAll(" ", "")
        String driver_hosts = params.CLIENT_HOSTS.replaceAll(" ", "")

        // Prepare additional parameters
        String test_options = params.TEST_OPTIONS + " warmup=" + params.CFG_WARMUP + " duration=" + params.CFG_DURATION + \
                " attempts=" + params.CFG_MAX_RETRY_ATTEMPTS
        // Remove whitespaces at the start and the end of the string
        test_options = test_options.replaceAll('(^\\s*|\\s*$)', "")
        String add_params = params.ADD_PARAMS + ' --to=' + test_options.split('\\s+').join(' --to=')

        String artifacts_remote_dir = "/jenkins/benchmarks/${BUILD_NUMBER}"

        String[] wal_mode_array = wal_mode.split(',')
        String[] sync_mode_array = sync_mode.split(',')
        String[] cache_mode_array = cache_mode.split(',')
        String[] pds_on_off_array = pds_on_off.split(',')
        String[] ssl_on_off_array = ssl_on_off.split(',')

        String attrib = ''
        if (!"".equals(params.ATTRIB)) {
            attrib = '--attr=' + params.ATTRIB.split(',').join(' --attr=') + ' '
        }
        if (!"".equals(params.ATTRIB_CUSTOM)) {
            attrib = attrib + '--attr=' + params.ATTRIB_CUSTOM.split(',').join(' --attr=') + ' '
        }

        def test_num = 1

        test_num_total = 0
        for (pds_mode in pds_on_off_array) {
            if (pds_mode == 'true'){
                test_num_total += sync_mode_array.size() * wal_mode_array.size() * ssl_on_off_array.size() * cache_mode_array.size()
            } else {
                test_num_total += sync_mode_array.size() * ssl_on_off_array.size() * cache_mode_array.size()
            }
        }

        def errors = 0
        try {
            for (curr_pds_mode in pds_on_off_array) {
                String[] wal_mode_array_0

                if (curr_pds_mode == 'false') {
                    wal_mode_array_0 = ['NONE']
                } else {
                    wal_mode_array_0 = wal_mode_array
                }

                for (curr_wal_mode in wal_mode_array_0) {
                    for (curr_cache_mode in cache_mode_array) {
                        for (curr_sync_mode in sync_mode_array) {
                            for (curr_ssl_mode in ssl_on_off_array) {
                                echo "Starting test ${test_num} out of ${test_num_total}. " + \
                                    "Persistence enabled: ${curr_pds_mode}, " + \
                                    "WAL mode: ${curr_wal_mode}, " + \
                                    "Sync mode: ${curr_sync_mode}, " + \
                                    "Cache mode: ${curr_cache_mode}, " + \
                                    "SSL mode: ${curr_ssl_mode}, " + \
                                    "Record JFR: ${params.RECORD_JFR}, " + \
                                    "Record Flamegraph: ${params.RECORD_FLAMEGRAPH}"

                                // Run the downstream job
                                def retval = build job: CHILD_JOB, parameters: [
                                    gitParameter(name: 'TIDEN_BRANCH', value: params.TIDEN_BRANCH),
                                    string(name: 'SERVER_HOSTS', value: server_hosts),
                                    string(name: 'CLIENT_HOSTS', value: driver_hosts),
                                    string(name: 'ARTIFACTS_REMOTE_DIR', value: artifacts_remote_dir),
                                    string(name: 'ATTRIB', value: attrib),
                                    string(name: 'ATTR_MATCH', value: params.ATTR_MATCH),
                                    string(name: 'ADD_PARAMS', value: add_params),
                                    booleanParam(name: 'CFG_PERSISTENCE_ENABLED', value: curr_pds_mode.toBoolean()),
                                    string(name: 'CFG_SYNC_MODE', value: curr_sync_mode),
                                    string(name: 'CFG_CACHE_MODE', value: curr_cache_mode),
                                    string(name: 'CFG_WAL_MODE', value: curr_wal_mode),
                                    string(name: 'ECO_SYSTEM_COMMENT', value: params.ECO_SYSTEM_COMMENT),
                                    booleanParam(name: 'SSL_ENABLED', value: curr_ssl_mode.toBoolean()),
                                    booleanParam(name: 'RECORD_JFR', value: params.RECORD_JFR),
                                    booleanParam(name: 'RECORD_FLAMEGRAPH', value: params.RECORD_FLAMEGRAPH),
                                    booleanParam(name: 'UPLOAD_TO_ECO_SYSTEM', value: params.UPLOAD_TO_ECO_SYSTEM),
                                    booleanParam(name: 'UPLOAD_REPORT_TO_QA_FTP', value: params.UPLOAD_REPORT_TO_QA_FTP)
                                ],
                                // Do not fail current step on downstream job's failure
                                propagate: false
                            
                                echo "Downstream job result: " + retval.result + ". " + \
                                    "Elapsed time: " + retval.durationString

                                if (retval.result != "SUCCESS") {
                                    errors++
                                }

                                copyArtifacts filter: 'var/*.xml, var/*.yaml, var/*.txt',
                                        fingerprintArtifacts: true, projectName: CHILD_JOB,
                                    selector: specific(retval.id), optional: true

                                test_num += 1
                            }
                        }
                    }
                }
            }
            if (errors != 0) {
                if (errors == test_num_total) {
                    echo "All downstream jobs failed!"
                    currentBuild.result = "FAILURE"
                } else {
                    currentBuild.result = "UNSTABLE"
                }
            }
        } catch(Exception ex) {
            echo ex.toString()
        }
    }

    ////////////////////
    stage('Report') {
        echo "Reporting..."

        // Archive the artifacts
        archiveArtifacts 'var/**'

        // TODO: Publish JUnit test result report
        junit '**/xunit*.xml'
    }
}
