#!/bin/bash

run_tests.py \
    --ts=examples \
    --tc=config/env_mshonichev.yaml \
    --tc=config/plugins-example.yaml \
    --tc=config/artifacts-hz.yaml \
    --attr=hazelcast \
    --clean=tests

