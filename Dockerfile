# --- Stage 1: Build & Install ---
FROM public.ecr.aws/unocha/python:3.13-stable AS builder

# git is required for versioning via hatch-vcs
RUN apk add --no-cache git

COPY --from=ghcr.io/astral-sh/uv:0.10.12 /uv /uvx /bin/

WORKDIR /srv/hdx-resource-changedetection

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# 1. Copy only dependency locks first for layer caching
COPY pyproject.toml uv.lock ./

# 2. Install dependencies without the project code
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# 3. Copy the rest of the codebase
COPY . .

# 4. Build and install the project non-editably into the .venv
# This creates the _version.py file inside the built wheel in .venv/site-packages
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# 5. CRITICAL: Delete the raw source tree so it doesn't shadow the installed package!
# Other folders like `config/` or `docker/` will remain untouched.
# We also delete .git and tests to prevent bloating the final runtime image.
RUN rm -rf src/ .git/ tests/

# --- Stage 2: Final Runtime ---
FROM public.ecr.aws/unocha/python:3.13-stable

WORKDIR /srv/hdx-resource-changedetection

# 6. Copy the environment (which no longer contains the unbuilt src/ directory)
COPY --from=builder /srv/hdx-resource-changedetection /srv/hdx-resource-changedetection

# 7. Copy entrypoint to the root, exactly as the old Hatch Dockerfile did
COPY docker/entrypoint.sh /entrypoint.sh

# 8. Install system dependencies and create log directories (Matching old Dockerfile)
RUN apk add --no-cache gettext-envsubst && \
    mkdir -p /var/log/hdx-resource-changedetection && \
    chmod +x /entrypoint.sh

# 9. Prepend the .venv to the PATH
ENV PATH="/srv/hdx-resource-changedetection/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

ENTRYPOINT [ "/entrypoint.sh" ]

CMD ["-c", "print('HDX Resource Change Detection')"]
