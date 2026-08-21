ARG PYTHON_IMAGE=python:3.10-slim@sha256:a78e4529630cfe8c5199cafd6e0c28ee1579a13f86274396d8b6b2d80367aa3a
FROM ${PYTHON_IMAGE} AS build

WORKDIR /src
RUN python -m pip install --no-cache-dir "build==1.2.2.post1"
COPY pyproject.toml README.md LICENSE MANIFEST.in ./
COPY wallaby_hires ./wallaby_hires
RUN python -m build --wheel

FROM ${PYTHON_IMAGE}

COPY --from=build /src/dist/*.whl /tmp/
RUN python -m pip install --no-cache-dir --no-deps /tmp/*.whl \
    && rm /tmp/*.whl \
    && useradd --create-home --uid 10001 wallaby

USER wallaby
WORKDIR /work
ENTRYPOINT ["wallaby_hires"]
CMD ["--help"]
