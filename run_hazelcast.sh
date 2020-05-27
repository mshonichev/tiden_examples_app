#!/bin/bash

run_tests.py \
    --ts=examples \
    --tc=config/env_mshonichev.yaml \
    --tc=config/artifacts-gg-ult-fab.yaml \
    --tc=config/artifacts-apps.yaml \
    --attr=hazelcast \
    --clean=tests

