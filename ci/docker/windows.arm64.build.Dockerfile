ARG DOCKER_IMAGE_BASE_BUILD=rayproject/buildenv:windows-arm64
FROM $DOCKER_IMAGE_BASE_BUILD

ENV PYTHON=3.9

COPY . .
RUN bash ci/ray_ci/windows/build_base.sh
