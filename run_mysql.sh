#!/bin/bash

run_tests.py \
    --ts=examples \
    --tc=config/env_mshonichev.yaml \
    --tc=config/plugins-example.yaml \
    --tc=config/artifacts-mysql.yaml \
    --attr=mysql \
    --clean=tests

