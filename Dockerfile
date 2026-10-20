# Ember broker image. Build:  docker build -t ember:local .
# Stage 1 builds the shaded broker and CLI jars (tests are run elsewhere: mvn clean verify).
FROM maven:3-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -B -q -DskipTests package -pl ember-broker,ember-client -am

# Stage 2: a small runtime image with no build tools and no curl (the healthcheck is a tiny `ember healthcheck` CLI command).
FROM eclipse-temurin:21-jre
RUN useradd --system --create-home --uid 10001 ember && mkdir /data && chown ember /data
COPY --from=build /src/ember-broker/target/ember-broker-*-all.jar /app/ember-broker.jar
COPY --from=build /src/ember-client/target/ember-client-*-all.jar /app/ember-cli.jar
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod 0755 /app/entrypoint.sh
USER ember
VOLUME /data
EXPOSE 9092 8080
# 9092 = clients and peers (Raft), 8080 = admin HTTP (metrics, health, status; UNAUTHENTICATED: keep it on a trusted network)
HEALTHCHECK --interval=10s --timeout=8s --start-period=30s --retries=6 \
  CMD ["java", "-Xmx64m", "-jar", "/app/ember-cli.jar", "healthcheck", "--admin=localhost:8080"]
ENTRYPOINT ["/app/entrypoint.sh"]
