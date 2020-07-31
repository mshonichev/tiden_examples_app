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

from re import search, compile

from aiohttp import web
from aiohttp.web_request import Request

exception_start_line = compile("^((Caused by:* )*class [a-zA-Z0-9\._]+Exception: |java.lang.NullPointerException|java.lang.[a-zA-Z0-9\._]+(Exception|Error): )|\] Fail|\] Critical|Error: Failed|\(err\) Failed|: Failed")

exception_end_lines = [compile(p) for p in [
    "\.\.\. \d+ more",
    'at java\.lang\.Thread\.run\(Thread\.java:\d+\)',
    'at org.apache.ignite.startup.cmdline.CommandLineStartup.main\(CommandLineStartup.java:\d+\)',
    'at org\.apache\.ignite.spi.IgniteSpiThread\.run\(IgniteSpiThread\.java:\d+\)',
    'at org\.apache\.ignite\.testtools\.SimpleIgniteTestClient\.main\(SimpleIgniteTestClient\.java:\d+\)',
]]

async def _exception_in_files(file_content):
    exception_lines = []
    exception_sets = {}

    async def is_exception_end_line(line):
        result = False
        for exception_end_line in exception_end_lines:
            result = result or search(exception_end_line, line)
            if result:
                break
        return result

    async def is_exception_start_line(line):
        return search(exception_start_line, line)

    after_exception_end_line = False

    in_exception_section = False
    lines = file_content.split('\n')
    lines_count = len(lines)

    section_start_line_number = 0
    for line_idx, line in enumerate(lines):
        if in_exception_section:
            if await is_exception_end_line(line):
                in_exception_section = False
                after_exception_end_line = True
                exception_lines.append(line.rstrip())
        else:
            if await is_exception_start_line(line):
                in_exception_section = True
                if line.startswith('Caused by') and after_exception_end_line:
                    if exception_lines:
                        exception_lines = exception_lines[:-1]
                else:

                    if exception_lines:
                        exception_sets[section_start_line_number] = exception_lines
                    exception_lines = []
                    section_start_line_number = line_idx

            after_exception_end_line = False
        if in_exception_section:
            exception_lines.append(line.rstrip())

        if line_idx == lines_count - 1:
            if exception_lines:
                exception_sets[section_start_line_number] = exception_lines

    return exception_sets

async def find_exception(request: Request):
    parts = await request.multipart()
    part = await parts.next()
    content = await part.read()
    decoded_content = content.decode('utf-8')
    return web.json_response(await _exception_in_files(decoded_content))

app = web.Application(client_max_size=1024**6)
app.add_routes([
    web.post('/find_exception', find_exception),
]),
web.run_app(app, port=1111)

