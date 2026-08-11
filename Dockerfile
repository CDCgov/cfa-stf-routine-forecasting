FROM rocker/tidyverse:4.5.3

#
# General Build Args and Environment Variables
#

ENV XLA_FLAGS=--xla_force_host_platform_device_count=4

#
# Additional programming language compilers/interpreters
#

# Julia 1.11 from official image
COPY --from=julia:1.11 /usr/local/julia /usr/local/julia
ENV PATH="/usr/local/julia/bin:${PATH}"
ENV JULIA_DEPOT_DIR=/opt/julia-depot
ENV JULIA_DEPOT_PATH=${JULIA_DEPOT_DIR}:
ENV JULIA_CPU_TARGET=generic
ENV JULIA_PKG_PRECOMPILE_AUTO=0

# Python from https://docs.astral.sh/uv/guides/integration/docker/
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Some handy uv environment variables
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_CACHE_DIR=/root/.cache/uv/python

#
# Copy local dependencies into our container, then install them
#

# R package - stfroutineforecasting
COPY ./stfroutineforecasting /cfa-stf-routine-forecasting/stfroutineforecasting

# Julia environment for direct NowcastAutoGP runner
# Copy only Julia environment metadata first so dependency installation is cached
# independently of changes to pipeline source files. The full source tree is
# copied later.
COPY ./src/cfa/stf/routine/epiautogp/Project.toml \
     ./src/cfa/stf/routine/epiautogp/Manifest.toml \
     /cfa-stf-routine-forecasting/src/cfa/stf/routine/epiautogp/

# Set working directory
WORKDIR /cfa-stf-routine-forecasting

# Instantiate Julia dependencies into the image so the runtime container can run
# the EpiAutoGP subprocess without downloading packages. This is a script
# environment under the EpiAutoGP package, so we commit its Manifest.toml for a
# reproducible EpiAutoGP dependency set.
RUN mkdir -p "${JULIA_DEPOT_DIR}" \
    && julia --project=src/cfa/stf/routine/epiautogp -e 'using Pkg; Pkg.instantiate(); Pkg.precompile()' \
    && chmod -R a+rwX "${JULIA_DEPOT_DIR}"

# Install stfroutineforecasting
RUN Rscript -e "install.packages('pak')"
RUN Rscript -e "\
    pak::repo_add(hubverse = 'https://hubverse-org.r-universe.dev'); \
    pak::local_install('stfroutineforecasting', upgrade = FALSE) \
"

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
    uv sync --no-install-project --no-dev

#
# Copy in Python pipeline and orchestration files that frequently change
#

# Project files
COPY src ./src
COPY README.md ./README.md
COPY dagster_defs.py ./dagster_defs.py

# Install the local project now that its sources are present
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev
