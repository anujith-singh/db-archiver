FROM python:3.7-stretch

ENV PERCONA_TOOLKIT_VERSION 3.0.13

RUN apt-get update

RUN wget --quiet --output-document=/tmp/percona-toolkit.deb https://www.percona.com/downloads/percona-toolkit/${PERCONA_TOOLKIT_VERSION}/binary/debian/stretch/x86_64/percona-toolkit_${PERCONA_TOOLKIT_VERSION}-1.stretch_amd64.deb

# verifying download
COPY percona-toolkit-checksum percona-toolkit-checksum
RUN sha512sum -c percona-toolkit-checksum

RUN apt-get install -y /tmp/percona-toolkit.deb

# clean up
RUN rm /tmp/percona-toolkit.deb
RUN apt-get -y auto-remove
RUN rm -rf /var/lib/apt/lists/*

ENV APP_DIR /app
WORKDIR ${APP_DIR}
COPY . ${APP_DIR}

RUN pip install -r requirements.lock

# Provide execute access
RUN chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
