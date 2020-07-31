FROM python:3.8-alpine

ENV NG_CLI_ANALYTICS=ci
RUN apk --no-cache add gcc musl-dev
RUN pip install aiohttp && mkdir /data

COPY server.py /server.py

ENTRYPOINT ["python", "/server.py"]