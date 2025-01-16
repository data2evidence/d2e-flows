FROM ghcr.io/data2evidence/alp-dataflow-gen-base:develop as dbsvc-build

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

# Grant docker ownership to create properties file
RUN chown -R docker:docker ./liquibase/

RUN mkdir /output
RUN chown -R docker:docker /output

# Create folder to store synpuf1k CSVs
RUN mkdir -p /app/synpuf1k
RUN chown -R docker:docker /app/synpuf1k

# Create folder to store vocab CSVs
RUN mkdir -p /app/vocab
RUN chown -R docker:docker /app/vocab

# Create folder to store duckdb extensions for offline plugins
# version must match duckdb in requirements.txt
ENV duckdb_version 1.1.1
RUN mkdir -p /app/duckdb_extensions
WORKDIR /app/duckdb_extensions
RUN wget https://extensions.duckdb.org/v${duckdb_version}/linux_amd64_gcc4/postgres_scanner.duckdb_extension.gz \
    && wget https://extensions.duckdb.org/v${duckdb_version}/linux_amd64_gcc4/fts.duckdb_extension.gz \
    && gzip -d postgres_scanner.duckdb_extension.gz \
    && gzip -d fts.duckdb_extension.gz

# To support offline installation
# Create folder to store R packages installed during runtime for plugins which require custom R packages
ENV R_LIBS_USER_DIR /home/docker/plugins/R/site-library
RUN mkdir -p $R_LIBS_USER_DIR
RUN chown -R docker:docker $R_LIBS_USER_DIR

# Common Data Model
RUN Rscript -e "remotes::install_github('OHDSI/CommonDataModel@v5.4.1', quiet=FALSE, upgrade='never', force=TRUE, dependencies=FALSE, lib='$R_LIBS_USER_DIR')"

# Uncomment for installation of i2b2 source code
# # Add Apache Ant for i2b2 data model creation
# WORKDIR /app
# ENV ANT_VERSION=1.9.6
# ENV I2B2_TAG_NAME 1.8.1.0001

# RUN wget http://archive.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz \
#     && tar xvfvz apache-ant-${ANT_VERSION}-bin.tar.gz -C /opt \
#     && ln -sfn /opt/apache-ant-${ANT_VERSION} /opt/ant \
#     && sh -c 'echo ANT_HOME=/opt/ant >> /etc/environment' \
#     && ln -sfn /opt/ant/bin/ant /usr/bin/ant \
#     && rm apache-ant-${ANT_VERSION}-bin.tar.gz

# # Download i2b2 source code
# RUN wget https://github.com/i2b2/i2b2-data/archive/refs/tags/v$I2B2_TAG_NAME.tar.gz
# RUN tar -xzf v$I2B2_TAG_NAME.tar.gz
# # Change ownership to overwrite db.properties in flow definition
# RUN chown -R docker:docker /app/i2b2-data-$I2B2_TAG_NAME

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
