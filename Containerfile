#syntax=docker/dockerfile:1-labs

FROM rocker/tidyverse:4.5.2

ENV XLA_FLAGS=--xla_force_host_platform_device_count=4

#
# Additional programming language compilers/interpreters
#

# Julia 1.11 from official image
COPY --from=julia:1.11 /usr/local/julia /usr/local/julia
ENV PATH="/usr/local/julia/bin:${PATH}"

# Python from https://docs.astral.sh/uv/guides/integration/docker/
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Some handy uv environment variables
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_CACHE_DIR=/root/.cache/uv/python

#
# Copy local dependencies into our container, then install them
#

# R package - hewr
COPY ./hewr /cfa-stf-routine-forecasting/hewr

# Julia package - EpiAutoGP
COPY ./EpiAutoGP /cfa-stf-routine-forecasting/EpiAutoGP

# Set working directory
WORKDIR /cfa-stf-routine-forecasting

# Install hewr
RUN Rscript -e "install.packages('pak')"
RUN Rscript -e "pak::local_install('hewr', upgrade = FALSE)"

# Cache Julia packages and artifacts
RUN --mount=type=cache,target=/root/.julia \
    julia --project=EpiAutoGP -e 'using Pkg; Pkg.instantiate()'

#
# Bring in python project dependency information and set the virtual env
#

# Dependency information
COPY pyproject.toml ./pyproject.toml
COPY uv.lock ./uv.lock

# Set VIRTUAL_ENV variable at runtime
ENV VIRTUAL_ENV=/cfa-stf-routine-forecasting/.venv

# Create the virtual environment
RUN uv venv "${VIRTUAL_ENV}"

# Update PATH to use the selected venv at runtime
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Sync all python dependencies (excluding the local project itself)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project

#
# Copy in python pipeline and orchestration files that frequently change
#

# Project files
COPY pipelines ./pipelines
COPY README.md ./README.md

# Install the local project now that pipelines/ sources are present
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync

# Dagster
COPY dagster_defs.py ./dagster_defs.py

#
# General Build Args and Environment Variables
#

ARG GIT_COMMIT_SHA
ENV GIT_COMMIT_SHA=$GIT_COMMIT_SHA
ARG GIT_BRANCH_NAME
ENV GIT_BRANCH_NAME=$GIT_BRANCH_NAME
