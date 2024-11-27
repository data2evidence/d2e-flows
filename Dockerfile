FROM alpcr.azurecr.io/alp-dataflow-gen-base:develop as dbsvc-build

# Required for RPostgres R package
RUN apt-get install libpq5 libpq-dev -y --no-install-recommends && apt-get clean


ADD https://github.com/liquibase/liquibase/releases/download/v4.5.0/liquibase-4.5.0.tar.gz .
RUN mkdir -p ./liquibase/
RUN tar xvf liquibase-4.5.0.tar.gz -C ./liquibase/


FROM dbsvc-build AS final-build

WORKDIR /app

COPY --chown=docker:docker ./postgresql-42.3.1.jar ./inst/drivers/
COPY --chown=docker:docker  ./__init__.py .
COPY --chown=docker:docker ./init.R .

RUN mkdir /output
RUN chown -R docker:docker /output

# Create folder to store synpuf1k CSVs
RUN mkdir -p /app/synpuf1k
RUN chown -R docker:docker /app/synpuf1k

# Create folder to store vocab CSVs
RUN mkdir -p /app/vocab
RUN chown -R docker:docker /app/vocab

# Create folder to store R packages installed during runtime for plugins which require custom R packages
RUN mkdir -p /home/docker/plugins/R/site-library
RUN chown -R docker:docker /home/docker/plugins/R/site-library

# Add Apache Ant for i2b2 data model creation
ENV ANT_VERSION=1.9.6
RUN wget http://archive.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz \
    && tar xvfvz apache-ant-${ANT_VERSION}-bin.tar.gz -C /opt \
    && ln -sfn /opt/apache-ant-${ANT_VERSION} /opt/ant \
    && sh -c 'echo ANT_HOME=/opt/ant >> /etc/environment' \
    && ln -sfn /opt/ant/bin/ant /usr/bin/ant \
    && rm apache-ant-${ANT_VERSION}-bin.tar.gz

WORKDIR /app

# Create folder for duckdb database files
RUN mkdir ./duckdb_data
RUN chown docker:alp ./duckdb_data
RUN mkdir -p ./cdw-config/duckdb_data
RUN chown -R docker:alp ./cdw-config


COPY --chown=docker:docker --chmod=711 ./requirements.txt .
RUN pip install -r requirements.txt

COPY --chown=docker:docker --chmod=711 ./shared_utils shared_utils
COPY --chown=docker:docker --chmod=711 ./flows flows


USER docker