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

# Usage: copy-paste output of QA TC suite tests / ignored tab to file <file>
# run this script:
#
# bash convert-TC-muted-tests-to-JIRA-table.sh <file>

if [ "$1" = "" ]; then
  echo "Usage: $0 <file>"
  exit 0
fi

cat $1 \
  | sed -e '/^[[:space:]]*$/d' \
  | sed -e 's/— SKIPPED: //' \
  | sed -re 's/((GG|IGN)-[0-9]+).*/\1/' \
  | sed -e 'N;s/\n/ /' \
  | awk '{print "|"$1"| "$2}'


