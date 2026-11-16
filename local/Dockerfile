# Images for the pipeline workers. Build context is the repo root (see .dockerignore):
#   docker build -f local/Dockerfile --target <light|media|vision> .
#
#   light   builders, summarizer and the setup scripts     (no ML libraries)
#   media   frame extractor, audio worker                  (ffmpeg, OpenCV, faster-whisper)
#   vision  vision worker, CPU                             (torch, transformers, Florence-2, RapidOCR)
#   embed   embedder worker and the search script, CPU     (torch, sentence-transformers)
#
# Package lists mirror local/requirements.txt (transformers stays pinned to the same version).
# Python 3.10 on purpose: the release-candidate 3.11 breaks transformers.

FROM python:3.10-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# On a network that re-signs HTTPS, copy the CA certificate (a .crt file) into local/ca/ first.
COPY local/ca/ /usr/local/share/ca-certificates/
RUN update-ca-certificates
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
RUN pip install confluent-kafka pyyaml boto3 "pydantic>=2" "psycopg[binary]>=3.1" python-dotenv

FROM base AS media-deps
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN pip install ffmpeg-python opencv-python-headless numpy faster-whisper

FROM base AS vision-deps
# libgl1 and libglib2.0-0: OpenCV (pulled in by RapidOCR) needs them
RUN apt-get update && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip install transformers==4.49.0 einops timm pillow numpy rapidocr-onnxruntime

FROM base AS embed-deps
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install sentence-transformers numpy

# The code goes in last, so editing it does not rebuild the package layers above.
FROM scratch AS code
COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY local/config /app/local/config
COPY local/scripts /app/local/scripts
COPY local/sql /app/local/sql
COPY local/services /app/local/services

FROM base AS light
COPY --from=code /app /app
RUN pip install --no-deps /app
WORKDIR /app/local

FROM media-deps AS media
COPY --from=code /app /app
RUN pip install --no-deps /app
WORKDIR /app/local

FROM vision-deps AS vision
COPY --from=code /app /app
RUN pip install --no-deps /app
WORKDIR /app/local

FROM embed-deps AS embed
COPY --from=code /app /app
RUN pip install --no-deps /app
WORKDIR /app/local
