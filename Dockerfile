# --- Stage 1: Build & Install ---
FROM public.ecr.aws/unocha/python:3.13-stable AS builder

# 1. Install git so hatch-vcs/setuptools_scm can determine the project version
RUN apk add --no-cache git

# 2. Install uv directly from astral's official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Enable bytecode compilation for faster startups
ENV UV_COMPILE_BYTECODE=1
# Prevent uv from looking for a system python it can't modify
ENV UV_LINK_MODE=copy

# 3. Copy ONLY dependency files first to maximize Docker layer caching
COPY pyproject.toml uv.lock ./

# 4. Install dependencies into a .venv (without the project code yet)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# 5. Copy the rest of the code (Make sure your .dockerignore allows the .git folder!)
COPY . .

# 6. Sync the project itself (This builds your package using git for the version)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


# --- Stage 2: Final Runtime ---
FROM public.ecr.aws/unocha/python:3.13-stable

WORKDIR /srv/hdx-resource-changedetection

# 7. Copy the entire isolated virtual environment from the builder
COPY --from=builder /app/.venv /app/.venv

# 8. Copy your application code and entrypoint
COPY . .
COPY docker/entrypoint.sh /

# 9. Setup system dependencies and permissions
RUN apk add --no-cache gettext-envsubst && \
    mkdir -p /var/log/hdx-resource-changedetection && \
    chmod +x /entrypoint.sh

# 10. Activate the virtual environment by placing its bin directory first in PATH
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

ENTRYPOINT [ "/entrypoint.sh" ]
CMD ["-c", "print('HDX Resource Change Detection')"]