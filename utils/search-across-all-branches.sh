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


# Usage: browse to the top dir of your local git repo and run this script:
#
# bash search-across-all-branches.sh <text-to-search>

if [ "$1" = "" ]; then
  echo "Usage: $0 <text-to-search>"
  exit 0
fi

git grep "$1" $(git rev-list --all) |xargs -i{} git  name-rev {} | cut -d' ' -f 2  | cut -d'~' -f 1 |  sort | uniq
