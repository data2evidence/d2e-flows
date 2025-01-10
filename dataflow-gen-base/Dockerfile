FROM alpine AS builder

# Create .Renviron file with GITHUB_PAT
ARG GITHUB_PAT
RUN echo "GITHUB_PAT=$GITHUB_PAT" >> .Renviron

FROM rocker/r-ver:4.4

COPY . /app
WORKDIR /app/fhirtoomop

WORKDIR /app
# GITHUB_PAT authentication for OHDSI remotes::install_github
COPY --from=builder .Renviron .

# GitHub deploy key ssh-config authentication for OHDSI remotes::install_github. host ssh-agent supplies private keys
COPY .github /root/

# installs
RUN apt-get update && \
	apt-get install -y --no-install-recommends \
		ca-certificates \
		cmake \
		curl \
		dirmngr \
		ed \
		gcc \
		git \
		less \
		libbz2-dev \
		libcairo2-dev \
		libcurl4-gnutls-dev \
		libfontconfig1-dev \
		libfreetype6-dev \
		libfribidi-dev \
		libgit2-dev \
		libharfbuzz-dev \
		libicu-dev \
		libjpeg-dev \
		libpng-dev \
		libpq-dev \
		libprotobuf-dev \
		libsodium23 libsodium-dev libsecret-tools \
		libssh2-1-dev \
		libssl-dev \
		libtiff-dev \
		libxml2-dev \
		libxt-dev \
		locales \
		openssh-client \
		protobuf-compiler \
		python3 \
		python3-dev \
		python3-pip \
		python3-venv \
		software-properties-common \
		sqlite3 \
		vim-tiny \
		wget \
		&& apt-get clean

# GitHub setup & test
RUN mkdir -p -m 700 ~/.ssh/ && ssh-keyscan github.com | tee -a ~/.ssh/known_hosts
RUN --mount=type=ssh ssh-add -l # debug only
RUN --mount=type=ssh git ls-remote git@github.com:data2evidence/d2e-DatabaseConnector | grep HEAD || exit 1
RUN --mount=type=ssh git ls-remote git@github.com:data2evidence/d2e-SqlRender | grep HEAD || exit 1
RUN git config --global advice.detachedHead false

# Python Library Setup
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 20
RUN python3 -m venv /usr/src/app
# Enable venv
ENV PATH="/usr/src/app/bin:$PATH"
RUN pip3 install -r /app/dataflow-gen-base/requirements.txt

# Set environment variables
ENV JAVA_HOME_11=/usr/lib/jvm/openjdk-11
ENV JAVA_HOME=/usr/lib/jvm/openjdk-17
ENV PATH="$JAVA_HOME/bin::/root/.cargo/bin:$PATH"
ENV LD_LIBRARY_PATH=/usr/lib/jvm/openjdk-11/lib/server
ENV R_REMOTES_UPGRADE="always"

ENV APP_VERSION=0.0.0
ENV PRING_PROFILES_ACTIVE=prod

# OpenJDK 17
RUN arch="$(dpkg --print-architecture)"; case "$arch" in 'amd64') downloadUrl='https://github.com/AdoptOpenJDK/openjdk17-binaries/releases/download/jdk-2021-05-07-13-31/OpenJDK-jdk_x64_linux_openj9_2021-05-06-23-30.tar.gz'; ;; 'arm64') downloadUrl='https://github.com/AdoptOpenJDK/openjdk17-binaries/releases/download/jdk-2021-05-07-13-31/OpenJDK-jdk_aarch64_linux_openj9_2021-05-06-23-30.tar.gz'; ;; *) echo >&2 "error: unsupported architecture: '$arch'"; exit 1 ;; esac; wget --progress=dot:giga -O openjdk.tgz "$downloadUrl"; mkdir -p ${JAVA_HOME}; tar --extract --file openjdk.tgz --directory "$JAVA_HOME" --strip-components 1 --no-same-owner ;

RUN rm openjdk.tgz*

# OpenJDK 11
RUN arch="$(dpkg --print-architecture)"; case "$arch" in 'amd64') downloadUrl='https://github.com/AdoptOpenJDK/openjdk11-upstream-binaries/releases/download/jdk-11.0.16%2B8/OpenJDK11U-jdk_x64_linux_11.0.16_8.tar.gz'; ;; 'arm64') downloadUrl='https://github.com/AdoptOpenJDK/openjdk11-upstream-binaries/releases/download/jdk-11.0.16%2B8/OpenJDK11U-jdk_aarch64_linux_11.0.16_8.tar.gz'; ;; *) echo >&2 "error: unsupported architecture: '$arch'"; exit 1 ;; esac; wget --progress=dot:giga -O openjdk.tgz "$downloadUrl"; mkdir -p ${JAVA_HOME_11}; tar --extract --file openjdk.tgz --directory "$JAVA_HOME_11" --strip-components 1 --no-same-owner ;

RUN update-alternatives --install /usr/bin/java java ${JAVA_HOME_11}/bin/java 100 && \
  update-alternatives --install /usr/bin/javac javac ${JAVA_HOME_11}/bin/javac 100 && \
  rm openjdk.tgz*

# Verify the installation
RUN java -version && R CMD javareconf

# Install Dependencies
RUN Rscript -e 'options(install.packages.compile.from.source = "never")'
RUN Rscript -e 'install.packages(c("remotes", "devtools", "rJava", "usethis"))'

# Install OHDSI Packages - sequence per dependencies

# HADES Dependencies
# https://github.com/r-lib/remotes https://search.r-project.org/CRAN/refmans/remotes/html/install_git.html
RUN Rscript -e 'remotes::install_github("OHDSI/Andromeda@v0.6.4")' # https://github.com/OHDSI/Andromeda
RUN Rscript -e 'remotes::install_github("OHDSI/BrokenAdaptiveRidge@v1.0.0")' # https://github.com/OHDSI/BrokenAdaptiveRidge
RUN Rscript -e 'remotes::install_github("OHDSI/Cyclops@v3.4.0")' # https://github.com/OHDSI/Cyclops
RUN Rscript -e 'remotes::install_github("rstudio/reticulate@v1.35.0")' # https://github.com/rstudio/reticulate
# RUN Rscript -e 'remotes::install_github("OHDSI/Achilles@v1.7.2")' # https://github.com/OHDSI/Achilles same version installed by HADES
# RUN Rscript -e 'remotes::install_github("OHDSI/DataQualityDashboard@v2.6.0", upgrade = "never")' # https://github.com/OHDSI/DataQualityDashboard same version installed by HADES

# # print packages and versions - before HADES
# RUN Rscript -e 'as.data.frame(installed.packages())[,c("Package", "Version")]' | awk '{print $1,$NF}' | grep -E "$(grep -l -i ohdsi /usr/local/lib/R/site-library/*/DESCRIPTION | awk -F/ '{print $(NF-1)}' | paste -sd '|')"

# OHDSI HADES
# private d2e repo auth - ssh key
# public HADES repo auth - .Renviron:GITHUB_PAT - https://ohdsi.github.io/Hades/rSetup.html#GitHub_Personal_Access_Token
RUN --mount=type=ssh Rscript -e 'remotes::install_github("OHDSI/Hades@v1.13.0")' # https://github.com/OHDSI/Hades https://ohdsi.github.io/Hades/installingHades.html
RUN --mount=type=ssh Rscript -e 'remotes::install_github("OHDSI/CohortMethod@v5.2.0")'
RUN --mount=type=ssh Rscript -e 'remotes::install_github("OHDSI/CohortIncidence@v3.3.0")'

# OHDSI packages and versions - after HADES
RUN Rscript -e 'as.data.frame(installed.packages())[,c("Package", "Version")]' | awk '{print $1,$NF}' | grep -E "$(grep -l -i ohdsi /usr/local/lib/R/site-library/*/DESCRIPTION | awk -F/ '{print $(NF-1)}' | paste -sd '|')"

# OHDSI packages and versions - after D2E
RUN Rscript -e 'as.data.frame(installed.packages())[,c("Package", "Version")]' | awk '{print $1,$NF}' | grep -E "$(grep -l -i ohdsi /usr/local/lib/R/site-library/*/DESCRIPTION | awk -F/ '{print $(NF-1)}' | paste -sd '|')"

RUN --mount=type=ssh Rscript -e 'remotes::install_github("OHDSI/Strategus@v0.2.1", upgrade = "never")' # Requires latest DatabaseConnector https://github.com/OHDSI/Strategus
RUN --mount=type=ssh Rscript -e 'devtools::install_github("OHDSI/CommonDataModel@v5.4.1", upgrade = "never", verbose = "true")' # https://github.com/OHDSI/CommonDataModel

# D2E packages
RUN --mount=type=ssh Rscript -e 'remotes::install_git("git@github.com:data2evidence/d2e-SqlRender@alp-dqd", force = TRUE)' # overwrite existing from OHDSI
RUN --mount=type=ssh Rscript -e 'remotes::install_git("git@github.com:data2evidence/d2e-DatabaseConnector@alp-dqd", force = TRUE)' # overwrite existing from OHDSI

# OHDSI packages and versions - final
RUN Rscript -e 'as.data.frame(installed.packages())[,c("Package", "Version")]' | awk '{print $1,$NF}' | grep -E "$(grep -l -i ohdsi /usr/local/lib/R/site-library/*/DESCRIPTION | awk -F/ '{print $(NF-1)}' | paste -sd '|')"

RUN rm -rf .Renviron /app/.github/.ssh

RUN useradd docker -u 1001 \
  && groupadd alp \
  && mkdir /home/docker \
  && chown docker:docker /home/docker \
  && adduser docker staff \
  && adduser docker alp