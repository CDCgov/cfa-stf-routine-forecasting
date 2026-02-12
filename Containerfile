#syntax=docker/dockerfile:1-labs

FROM rocker/tidyverse

ARG GIT_COMMIT_SHA
ENV GIT_COMMIT_SHA=$GIT_COMMIT_SHA

ARG GIT_BRANCH_NAME
ENV GIT_BRANCH_NAME=$GIT_BRANCH_NAME

ENV XLA_FLAGS=--xla_force_host_platform_device_count=4

COPY ./hewr /cfa-stf-routine-forecasting/hewr
COPY ./EpiAutoGP /cfa-stf-routine-forecasting/EpiAutoGP

WORKDIR /cfa-stf-routine-forecasting

RUN Rscript -e "install.packages('pak')"
RUN Rscript -e "pak::local_install('hewr', upgrade = FALSE)"

# TODO: Weigh cache performance vs containerfile simplificty;
# We can specify files and folders individually for better performance
COPY . .

# Python from https://docs.astral.sh/uv/guides/integration/docker/
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# Julia 1.11 from official image
COPY --from=julia:1.11 /usr/local/julia /usr/local/julia
ENV PATH="/usr/local/julia/bin:${PATH}"

# Some handy uv environment variables
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_CACHE_DIR=/root/.cache/uv/python

# Set VIRTUAL_ENV variable at runtime
ENV VIRTUAL_ENV=/cfa-stf-routine-forecasting/.venv

RUN uv venv $(VIRTUAL_ENV)

# Update PATH to use the selected venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync

# Cache Julia packages and artifacts
RUN --mount=type=cache,target=/root/.julia \
    julia --project=EpiAutoGP -e 'using Pkg; Pkg.instantiate()'
