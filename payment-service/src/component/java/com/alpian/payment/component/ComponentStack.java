package com.alpian.payment.component;

import java.nio.file.Path;
import java.time.Duration;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

/**
 * The whole system in containers, shared by every component test. Both services run from their
 * Dockerfiles; topics come from the compose stack's {@code create-topics.sh}.
 */
final class ComponentStack {

  static final int PAYMENT_PORT = 8080;
  static final int NOTIFICATION_PORT = 8081;

  private static final String KAFKA_IMAGE = "apache/kafka:3.8.0";

  private static final String INTERNAL_KAFKA = "kafka:19092";

  static final Network NETWORK = Network.newNetwork();

  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withNetwork(NETWORK)
          .withNetworkAliases("postgres")
          .withDatabaseName("payment")
          .withUsername("payment")
          .withPassword("payment");

  static final KafkaContainer KAFKA =
      new KafkaContainer(KAFKA_IMAGE)
          .withNetwork(NETWORK)
          .withNetworkAliases("kafka")
          .withListener(INTERNAL_KAFKA);

  static final GenericContainer<?> PAYMENT_SERVICE =
      service("payment-service", PAYMENT_PORT)
          .withEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/payment")
          .withEnv("POSTGRES_USER", "payment")
          .withEnv("POSTGRES_PASSWORD", "payment")
          // So the outage test sees the relay retry within seconds.
          .withEnv("PAYMENT_OUTBOX_MAX_BACKOFF", "2s");

  static final GenericContainer<?> NOTIFICATION_SERVICE =
      service("notification-service", NOTIFICATION_PORT)
          .withEnv("SPRING_KAFKA_STREAMS_PROPERTIES_NUM_STANDBY_REPLICAS", "0");

  static {
    Startables.deepStart(POSTGRES, KAFKA).join();
    createTopics();
    Startables.deepStart(PAYMENT_SERVICE, NOTIFICATION_SERVICE).join();
  }

  private ComponentStack() {}

  static String paymentServiceUrl() {
    return "http://"
        + PAYMENT_SERVICE.getHost()
        + ":"
        + PAYMENT_SERVICE.getMappedPort(PAYMENT_PORT);
  }

  /** Freezes the broker: connections hang, as in a network partition. */
  static void pauseKafka() {
    DockerClientFactory.instance().client().pauseContainerCmd(KAFKA.getContainerId()).exec();
  }

  static void resumeKafka() {
    DockerClientFactory.instance().client().unpauseContainerCmd(KAFKA.getContainerId()).exec();
  }

  private static GenericContainer<?> service(String name, int port) {
    Path moduleDir = Path.of(System.getProperty("component." + name + ".dir"));
    return new GenericContainer<>(
            new ImageFromDockerfile("alpian/" + name + "-component", false)
                .withDockerfile(moduleDir.resolve("Dockerfile")))
        .withNetwork(NETWORK)
        .withEnv("SPRING_KAFKA_BOOTSTRAP_SERVERS", INTERNAL_KAFKA)
        .withExposedPorts(port)
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(name)).withPrefix(name))
        .waitingFor(
            Wait.forHttp("/actuator/health/readiness")
                .forPort(port)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private static void createTopics() {
    try (GenericContainer<?> topics =
        new GenericContainer<>(KAFKA_IMAGE)
            .withNetwork(NETWORK)
            .withEnv("BOOTSTRAP_SERVER", INTERNAL_KAFKA)
            .withCopyFileToContainer(
                MountableFile.forHostPath(System.getProperty("component.create-topics")),
                "/create-topics.sh")
            .withCreateContainerCmdModifier(
                cmd -> cmd.withEntrypoint("/bin/bash", "/create-topics.sh"))
            .withStartupCheckStrategy(
                new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(90)))) {
      topics.start();
    }
  }
}
