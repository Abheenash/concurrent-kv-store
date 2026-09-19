FROM gcc:14 AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j && ctest --test-dir build --output-on-failure

FROM debian:bookworm-slim
COPY --from=build /src/build/kvserver /src/build/kvbench /usr/local/bin/
VOLUME /data
EXPOSE 5555
ENTRYPOINT ["kvserver"]
CMD ["--port", "5555", "--mode", "poll", "--aof", "/data/kv.aof", "--fsync", "everysec"]
