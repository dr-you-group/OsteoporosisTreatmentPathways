# Rstudio 4.2.1 and Atlas 2.8.0 Version
FROM rocker/rstudio:4.2.1

RUN apt-get update && \
    apt-get install -y default-jdk

RUN R CMD javareconf
RUN R -e "install.packages('rJava', dependencies = TRUE)"

# Git Token
ARG GIT_ACCESS_TOKEN
RUN echo "GITHUB_PAT=$GIT_ACCESS_TOKEN" >> /usr/local/lib/R/etc/Renviron

# install devtools
RUN apt-get update
RUN apt-get install -y \
    build-essential \ 
    libcurl4-gnutls-dev \
    libxml2-dev \
    libssl-dev \
    libgit2-dev \
# install other
    libfontconfig1-dev \
    libcairo2-dev \
    libsodium-dev

RUN R CMD javareconf

RUN R -e 'install.packages("dplyr")'
RUN R -e 'install.packages("DT")'
RUN R -e 'install.packages("lubridate")'
RUN R -e 'install.packages("renv")'
RUN R -e 'install.packages("shiny")'

# OHDSI Packages
RUN R -e "install.packages('devtools')"
RUN R -e 'devtools::install_github("OHDSI/Andromeda",ref="v0.3.2")'
RUN R -e 'devtools::install_github("OHDSI/CohortMethod",ref="v4.0.0")'
RUN R -e 'devtools::install_github("OHDSI/Cyclops",ref="v3.0.0")'
RUN R -e 'devtools::install_github("OHDSI/DatabaseConnector",ref="v3.0.0")'
RUN R -e 'devtools::install_github("OHDSI/DatabaseConnectorJars",ref="v1.1.0")'
RUN R -e 'devtools::install_github("OHDSI/EmpiricalCalibration",ref="v2.0.2")'
RUN R -e 'devtools::install_github("OHDSI/FeatureExtraction",ref="v3.0.1")'
RUN R -e 'devtools::install_github("OHDSI/MethodEvaluation",ref="v2.0.0")'
RUN R -e 'devtools::install_github("OHDSI/OhdsiSharing",ref="v0.2.2")'
RUN R -e 'devtools::install_github("OHDSI/ParallelLogger",ref="v2.0.0")'
RUN R -e 'devtools::install_github("OHDSI/SqlRender",ref="v1.6.8")'

RUN mkdir -p /home/jdbc
COPY ./jdbc /home/jdbc