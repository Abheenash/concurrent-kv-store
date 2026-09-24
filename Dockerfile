FROM gcc:14 AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j && ctest --test-dir build --output-on-failure

FROM debian:bookworm-slim
COPY --from=build /src/build/kvserver /src/build/kvbench /usr/local/bin/

# Run as an unprivileged user. This was the only container in my repos still
# running as root, and it is the one that least should be: it is a network server
# that listens on a public port and parses a text protocol from untrusted input.
# A parsing bug in a root process is a host compromise; the same bug under uid
# 10001 is a crash.
#
# /data has to be owned by that user because the append-only file is written
# there at runtime. Creating it here rather than relying on the VOLUME means the
# permissions are right even when the volume is bind-mounted from the host.
RUN groupadd --system --gid 10001 kv \
 && useradd --system --uid 10001 --gid kv --no-create-home --shell /usr/sbin/nologin kv \
 && mkdir -p /data && chown kv:kv /data
USER 10001

VOLUME /data
EXPOSE 5555
ENTRYPOINT ["kvserver"]
CMD ["--port", "5555", "--mode", "poll", "--aof", "/data/kv.aof", "--fsync", "everysec"]
