#syntax=docker/dockerfile:1-labs

FROM rocker/tidyverse

#
# Gemeral Build Args and Environment Variables
#

ARG GIT_COMMIT_SHA
ENV GIT_COMMIT_SHA=$GIT_COMMIT_SHA
ARG GIT_BRANCH_NAME
ENV GIT_BRANCH_NAME=$GIT_BRANCH_NAME
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
# Copy python project files explicitly, and set the virtual environment
#

# Project files
COPY pipelines .
COPY pyproject.toml .
COPY uv.lock .
COPY README.md .

# Set VIRTUAL_ENV variable at runtime
ENV VIRTUAL_ENV=/cfa-stf-routine-forecasting/.venv

RUN uv venv $(VIRTUAL_ENV)

# Update PATH to use the selected venv at runtime
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Sync all python dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync
