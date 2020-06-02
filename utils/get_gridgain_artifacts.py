#!/usr/bin/env python3
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

from os import rename
from ftplib import FTP
from urllib.parse import urlparse

import sys
import argparse
import re
import requests


class Artifact:
    name_re_tmpl = r'^(gridgain-{}{})(-fabric)?-{}(-SNAPSHOT)?(-bin)?([\.-][bpt]\d+)?\.zip$'

    @staticmethod
    def get_name_re(art_type, ver=r'(\d+\.\d+\.\d+)'):
        if art_type in ('professional', 'community'):
            return Artifact.name_re_tmpl.format('(professional|community)', '|apache-ignite', ver)
        else:
            return Artifact.name_re_tmpl.format(art_type, '', ver)


class QAFtp:

    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.address = '172.25.2.50'
        self.url = f'ftp://{self.address}'
        self.ftp = FTP(self.address, self.login, self.password)

    def download_by_version(self, ver, artifact_type):
        ftp_path = self.search_by_version(ver, artifact_type)
        if not ftp_path:
            print(f"ERROR! Cannot find artifact version {ver} of type {artifact_type}")
            sys.exit(2)
        return self.download(ftp_path, ftp_path.split('/')[-1])

    def search_by_version(self, ver, artifact_type):
        """
        Walk through predefined paths on FTP and find an artifact of given version
        :param ver: version to search, e.g. 8.7.16
        :param artifact_type:
        :return: a path to artifact relative to FTP root. None if no artifact found
        """
        artifact_name_re = Artifact.get_name_re(artifact_type, ver)
        print(f"Search regex: {artifact_name_re}")

        for path in (ver + '/dev', 'releases'):
            print(f"Searching in {self.url}/{path}...")
            for ls in self.ftp.mlsd(path):
                if ls[1]['type'] == 'file' and \
                        re.search(artifact_name_re, ls[0]):
                    print(f"Found {ls[0]}")
                    return f"{path}/{ls[0]}"
        return None

    def download(self, path, dst_file):
        """
        Download
        :param path: a file path to download relative to FTP root
        :param dst_file: filename to download into
        :return: dst_file
        """
        print(f"Downloading {path} to {dst_file}...")

        with open(dst_file, 'wb') as f:
            self.ftp.retrbinary('RETR ' + path, f.write)

        return dst_file


class GGTeamCity:

    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.base_url = 'https://ggtc.gridgain.com'
        self.tc_build_job_ids = (
            'Releases_GridGainCeEeUe_QuickAssemblyGridGainCe',      # 8.7.3+, CE only
            'Releases_GridGainCeEeUe_QuickAssemblyGridGainCeEeUe',  # 8.7.3+
            'Releases_GridGainPeEeUe_8_4_1__8_7_2_RunGridGainBuild8xQuickAssembly'  # 8.4.1-8.7.2
        )

    def download_by_branch(self, build_branch, art_type):
        """
        Search and download the latest artifact built on TC from a given GG branch
        :param build_branch:
        :param art_type:
        :return:
        """
        artifact_path = None

        art_filter_re = Artifact.get_name_re(art_type)

        # Find artifacts on TC
        for build_job_id in self.tc_build_job_ids:
            artifacts = self.get_artifacts_list(build_job_id, build_branch, auth=(self.login, self.password))
            if not artifacts:
                continue

            artifacts_filtered = list(filter(
                lambda a: re.search(art_filter_re, a.get('name', '')),
                artifacts
            ))
            if not artifacts_filtered:
                continue

            artifact_meta = artifacts_filtered.pop()
            artifact_path = artifact_meta.get('content', {}).get('href')

            if artifact_path:
                print(f"Build found. Modification time: {artifact_meta.get('modificationTime', 'N/A')}")
                break

        if not artifact_path:
            print(f"ERROR! Cannot find a build from branch '{build_branch}' of type '{art_type}' here:")
            for build_id in self.tc_build_job_ids:
                print(f"- {self.base_url}/viewType.html?buildTypeId={build_id}")
            sys.exit(1)

        artifact_basename = artifact_path.split('/')[-1]

        return self.download_artifact(artifact_path, artifact_basename)

    def get_artifacts_list(self, job_id, branch, **kwargs):
        """
        Get all most recent artifacts built from a given branch. Use TC REST API
        :param job_id: build name
        :param branch: build branch
        :param kwargs:
        :return: a link to artifact relative to TC root
        """
        params = {'headers':
                      {'Accept': 'application/json', 'charset': 'UTF-8'}
                  }
        if kwargs.get('auth'):
            params['auth'] = kwargs['auth']

        # https://www.jetbrains.com/help/teamcity/2019.2/rest-api.html#Build-Configuration-Locator
        url = self.base_url + f'/httpAuth/app/rest/builds/buildType:(id:{job_id}),branch:{branch},status:SUCCESS/artifacts'
        print(f"Trying build URL: {url}... ", end='')
        r = requests.get(url, **params)
        print(r.status_code)

        if r.status_code != requests.codes.ok:
            return []

        return r.json().get('file', [])

    def download_artifact(self, path, basename):
        """
        Download artifact from TC
        :param path: a path to artifact relative to the TC root
        :param basename: a filename to download into
        :return: basename
        """
        url = self.base_url + path
        print(f"Downloading {url} to {basename}...")

        r = requests.get(url, auth=(self.login, self.password), stream=True)
        r.raise_for_status()

        with open(basename, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)

        return basename


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--search", help="Search string (version, branch, url)", required=True)
    parser.add_argument("--type", default="ultimate",
                        choices=['professional', 'community', 'enterprise', 'ultimate'],
                        help="Artifact type")
    parser.add_argument("--tc_login", help="TeamCity login", required=True)
    parser.add_argument("--tc_password", help="TeamCity password", required=True)
    parser.add_argument("--ftp_login", help="QA FTP login", required=True)
    parser.add_argument("--ftp_password", help="QA FTP password", required=True)
    parser.add_argument("--prefix", help="Add prefix to destination filename")

    return parser.parse_args()

# Main

def main():
    args = parse_args()
    search_str = args.search
    art_type = args.type
    dst_prefix = args.prefix

    print(f"Artifact type: {art_type}")
    print(f"Search string: {search_str}")

    qa_ftp = QAFtp(args.ftp_login, args.ftp_password)
    tc = GGTeamCity(args.tc_login, args.tc_password)

    dst_file = None

    if re.search(r"\.zip$", search_str):
        # Get path relative to FTP root
        rel_path = urlparse(search_str).path
        dst_file = qa_ftp.download(rel_path, rel_path.split('/')[-1])
    elif re.search(r"^[289]\.\d+\.\d+", search_str):
        print("Will search an artifact on QA FTP by version")
        dst_file = qa_ftp.download_by_version(search_str, art_type)
    else:
        print(f"Will search an artifact on {tc.base_url} by branch")
        dst_file = tc.download_by_branch(search_str, art_type)

    # Add prefix to target filename
    if dst_prefix:
        new_dst_file = f'{dst_prefix}-{dst_file}'
        print(f"Renaming {dst_file} to {new_dst_file}")
        rename(dst_file, new_dst_file)

if __name__ == '__main__':
    main()
