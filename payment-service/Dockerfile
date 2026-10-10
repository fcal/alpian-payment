# Build the jar first: ./gradlew :payment-service:bootJar
# Not Alpine: Kafka Streams' RocksDB needs glibc. Both services share the base image.
FROM eclipse-temurin:21-jre

RUN groupadd --system app && useradd --system --gid app --no-create-home app
USER app
WORKDIR /app

COPY build/libs/payment-service.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
