# Runtime image for the notification-service. Build the jar first:
#
#   ./gradlew :notification-service:bootJar && docker build -t notification-service notification-service
#
# A JRE rather than a JDK, and a non-root user: the image needs to run the service and nothing
# else. Not the Alpine variant: Kafka Streams' state store is RocksDB, whose native library links
# against glibc's libstdc++, and on Alpine's musl it fails to load -- every stream thread dies on
# its first store access. Both services use the same base so they are patched together.
FROM eclipse-temurin:21-jre

RUN groupadd --system app && useradd --system --gid app --no-create-home app
USER app
WORKDIR /app

COPY build/libs/notification-service.jar app.jar

EXPOSE 8081
# Size the heap from the container's memory limit rather than the host's.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
