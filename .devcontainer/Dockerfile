FROM mcr.microsoft.com/devcontainers/base:ubuntu

USER root

# Create fast caches and venv location with user ownership
ARG USER=vscode
RUN mkdir -p /venv /pycache && chown -R ${USER}:${USER} /venv /pycache

ENV PATH="/venv/bin:$PATH"

RUN sudo apt-get update && sudo apt-get -y upgrade

# Install uv by copying the binary from the official distroless Docker imag
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install just (binary release from GitHub)
RUN curl -sSL "https://just.systems/install.sh" | bash -s -- --to /usr/local/bin

# Install direnv (Debian package is fine; alternatively build from source)
RUN apt-get update && \
    apt-get install -y --no-install-recommends direnv && \
    rm -rf /var/lib/apt/lists/*

# Make sure direnv is hooked into shells
RUN echo 'eval "$(direnv hook bash)"' >> /etc/bash.bashrc && \
    echo 'eval "$(direnv hook zsh)"' >> /etc/zsh/zshrc
