package dev.thiagogomes.fraud.detector.alert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import dev.thiagogomes.fraud.detector.config.FraudRulesProperties;
import dev.thiagogomes.fraud.detector.transaction.Transaction;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class AlertApiIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");
    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
            .withStartupTimeout(Duration.ofMinutes(3));
    @TempDir static Path stateDir;
    // Changelogs de janelas têm retenção real; uma data fixa expira entre execuções.
    static final Instant BASE = Instant.now().minusSeconds(60);
    static final Clock CLOCK = Clock.fixed(BASE.plusSeconds(10), ZoneOffset.UTC);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.streams.state-dir", () -> stateDir.toString());
        registry.add("spring.kafka.streams.application-id", () -> "fraud-integration");
    }

    @Autowired MockMvc mvc;
    @Autowired AlertConsumer consumer;
    @Autowired AlertRepository repository;
    @Autowired FraudRulesProperties rules;
    @Autowired ApplicationContext context;
    @Autowired MeterRegistry meters;

    @BeforeEach
    void clearAlerts() { repository.deleteAll(); }

    @Test
    void migratesTheDatabaseAndBindsAllRuleProperties() {
        assertThat(repository.count()).isZero();
        assertThat(rules.highAmount().multiplier()).isEqualByComparingTo("5");
        assertThat(rules.impossibleTravel().maxSpeedKmh()).isEqualTo(900);
        assertThat(rules.cardTesting().window()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void listsFiltersPaginatesAndReadsDetails() throws Exception {
        FraudAlert velocity = alert(1, "card-api", "VELOCITY");
        consumer.consume(velocity);
        consumer.consume(alert(2, "card-other", "HIGH_AMOUNT"));
        mvc.perform(get("/alerts").param("rule", "VELOCITY").param("status", "OPEN").param("cardId", "card-api"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.content[0].id").value(velocity.id().toString()));
        mvc.perform(get("/alerts").param("size", "1").param("page", "1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.totalPages").value(2));
        mvc.perform(get("/alerts/{id}", velocity.id())).andExpect(status().isOk())
                .andExpect(jsonPath("$.details.count").value(6)).andExpect(jsonPath("$.status").value("OPEN"));
    }

    @Test
    void repeatedDeliveryPreservesTheAnalystStatusAndAllowsDifferentRules() throws Exception {
        FraudAlert alert = alert(3, "card-api", "VELOCITY");
        consumer.consume(alert);
        mvc.perform(patch("/alerts/{id}/status", alert.id()).contentType(MediaType.APPLICATION_JSON)
                .content("{\"status\":\"CONFIRMED\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("CONFIRMED"));
        consumer.consume(new FraudAlert(new UUID(99, 1), alert.transactionId(), alert.cardId(), alert.rule(),
                alert.amount(), alert.details(), alert.transactionAt(), alert.detectedAt()));
        assertThat(repository.count()).isEqualTo(1);
        mvc.perform(get("/alerts/{id}", alert.id())).andExpect(jsonPath("$.status").value("CONFIRMED"));
        consumer.consume(alert(3, "card-api", "HIGH_AMOUNT"));
        assertThat(repository.count()).isEqualTo(2);
        mvc.perform(patch("/alerts/{id}/status", alert.id()).contentType(MediaType.APPLICATION_JSON)
                .content("{\"status\":\"FALSE_POSITIVE\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("FALSE_POSITIVE"));
    }

    @Test
    void concurrentDeliveryDoesNotCreateDuplicates() throws Exception {
        FraudAlert alert = alert(4, "card-concurrent", "VELOCITY");
        try (var executor = Executors.newFixedThreadPool(4)) {
            var tasks = IntStream.range(0, 12).<Callable<Void>>mapToObj(i -> () -> {
                consumer.consume(alert);
                return null;
            }).toList();
            for (var result : executor.invokeAll(tasks)) result.get();
        }
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void invalidInputReturnsProblemDetailsWithoutStacktraces() throws Exception {
        for (String query : new String[] {"size=101", "size=0", "page=-1", "page=10001", "rule=UNKNOWN", "status=UNKNOWN",
                "cardId=" + "c".repeat(65), "size=2147483648"}) {
            mvc.perform(get("/alerts?" + query)).andExpect(status().isBadRequest())
                    .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                    .andExpect(jsonPath("$.trace").doesNotExist());
        }
        FraudAlert alert = alert(5, "card-api", "VELOCITY");
        consumer.consume(alert);
        for (String body : new String[] {"{}", "{\"status\":\"OPEN\"}", "{\"status\":\"INVALID\"}", "{"}) {
            mvc.perform(patch("/alerts/{id}/status", alert.id()).contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest()).andExpect(jsonPath("$.status").value(400));
        }
        mvc.perform(get("/alerts/{id}", new UUID(0, 999))).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
        mvc.perform(get("/alerts/not-a-uuid")).andExpect(status().isBadRequest());
    }

    @Test
    void bindsTheRealHttpServerToLoopbackByDefault() throws Exception {
        assertThat(context.getEnvironment().getProperty("server.address")).isEqualTo("127.0.0.1");
        int port = context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
        try (var client = java.net.http.HttpClient.newHttpClient()) {
            var response = client.send(java.net.http.HttpRequest.newBuilder(
                    java.net.URI.create("http://127.0.0.1:" + port + "/actuator/health")).GET().build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("UP").doesNotContain("password", "jdbc:");
        }
    }

    @Test
    void treatsInjectionAsDataAndPreventsMassAssignment() throws Exception {
        var alert = alert(71, "card-security", "VELOCITY");
        consumer.consume(alert);
        mvc.perform(get("/alerts").param("cardId", "' OR 1=1 --"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.totalElements").value(0));
        mvc.perform(patch("/alerts/{id}/status", alert.id()).contentType(MediaType.APPLICATION_JSON)
                .content("{\"status\":\"CONFIRMED\",\"amount\":0,\"cardId\":\"changed\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.amount").value(89.97))
                .andExpect(jsonPath("$.cardId").value("card-security"));
        mvc.perform(patch("/alerts/{id}/status", alert.id()).contentType(MediaType.TEXT_PLAIN)
                .content("CONFIRMED")).andExpect(status().isUnsupportedMediaType());
        mvc.perform(get("/alerts").header("Origin", "https://untrusted.example"))
                .andExpect(header().doesNotExist("Access-Control-Allow-Origin"));
    }

    @Test
    void rejectsAReusedAlertIdWithADifferentTransactionInsteadOfSilentlyLosingIt() {
        var first = alert(72, "card-collision", "VELOCITY");
        consumer.consume(first);
        var conflicting = new FraudAlert(first.id(), new UUID(0, 73), first.cardId(), first.rule(), first.amount(),
                first.details(), first.transactionAt(), first.detectedAt());
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException().isThrownBy(() -> consumer.consume(conflicting));
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void quarantinesMalformedFutureAndMismatchedTransactionsAndContinuesTheSamePartition() throws Exception {
        var json = tools.jackson.databind.json.JsonMapper.builder().build();
        String card = "quarantine-transactions";
        var future = new Transaction(new UUID(80, 1), card, new BigDecimal("89.97"), "Loja", "Cidade", 0, 0,
                Instant.now().plus(Duration.ofDays(365)));
        publishRaw("transactions", card, "{");
        publishRaw("transactions", card, json.writeValueAsString(future));
        publishRaw("transactions", card, json.writeValueAsString(new Transaction(new UUID(80, 2), "different-card",
                new BigDecimal("89.97"), "Loja", "Cidade", 0, 0, BASE)));
        for (int i = 0; i < 6; i++) {
            publishRaw("transactions", card, json.writeValueAsString(new Transaction(new UUID(81, i), card,
                    new BigDecimal("89.97"), "Loja", "Cidade", 0, 0, BASE.plusSeconds(20 + i))));
        }
        assertThat(readQuarantine("transactions-dlt", card, 3)).hasSize(3).contains("{");
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(repository.findAll()).anySatisfy(alert -> {
                    assertThat(alert.transactionId).isEqualTo(new UUID(81, 5));
                    assertThat(alert.rule).isEqualTo(FraudRule.VELOCITY);
                }));
    }

    @Test
    void quarantinesMalformedAlertsAndContinuesPersistenceInTheSamePartition() throws Exception {
        var json = tools.jackson.databind.json.JsonMapper.builder().build();
        var valid = alert(82, "quarantine-alerts", "VELOCITY");
        publishRaw("fraud-alerts", valid.cardId(), "{");
        publishRaw("fraud-alerts", valid.cardId(), json.writeValueAsString(valid).replace("VELOCITY", "UNKNOWN"));
        publishRaw("fraud-alerts", valid.cardId(), json.writeValueAsString(valid));
        assertThat(readQuarantine("fraud-alerts-dlt", valid.cardId(), 2)).hasSize(2).contains("{");
        await().atMost(Duration.ofSeconds(60)).until(() -> repository.existsById(valid.id()));
    }

    private void publishRaw(String topic, String key, String value) throws Exception {
        var config = Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all");
        try (var producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, 0, key, value)).get(30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private java.util.List<String> readQuarantine(String topic, String key, int count) {
        var config = Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "quarantine-verification-" + UUID.randomUUID(), "enable.auto.commit", false);
        try (var reader = new org.apache.kafka.clients.consumer.KafkaConsumer<>(config,
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new org.apache.kafka.common.serialization.StringDeserializer())) {
            var partitions = IntStream.range(0, 3).mapToObj(p -> new org.apache.kafka.common.TopicPartition(topic, p)).toList();
            reader.assign(partitions);
            reader.seekToBeginning(partitions);
            var values = new java.util.ArrayList<String>();
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                for (var record : reader.poll(Duration.ofMillis(300))) {
                    if (key.equals(record.key())) values.add(record.value());
                }
                return values.size() >= count;
            });
            return values;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"lz4", "snappy"})
    void publishesACompressedKafkaBurstAndPersistsTheDetectedAlert(String compression) throws Exception {
        var config = Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all", ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        String card = "card-e2e-" + compression;
        UUID sixth = new UUID(compression.hashCode(), 5);
        try (var producer = new KafkaProducer<>(config, new StringSerializer(), new JacksonJsonSerializer<Transaction>().noTypeInfo())) {
            for (int i = 0; i < 6; i++) {
                Transaction transaction = new Transaction(new UUID(compression.hashCode(), i), card, new BigDecimal("89.97"),
                        "Loja", "Curitiba", -25.43, -49.27, BASE.plusSeconds(i));
                producer.send(new ProducerRecord<>("transactions", transaction.cardId(), transaction)).get();
            }
        }
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            var alerts = repository.findAll();
            assertThat(alerts).singleElement().satisfies(alert -> {
                assertThat(alert.transactionId).isEqualTo(sixth);
                assertThat(alert.rule).isEqualTo(FraudRule.VELOCITY);
                assertThat(alert.status).isEqualTo(AlertStatus.OPEN);
            });
        });
    }

    @Test
    void restoresRuleHistoryFromKafkaAfterLosingLocalState() throws Exception {
        double processed = meters.get("fraud.transactions").counter().count();
        publish("card-recovery", 20, 0, 5);
        await().atMost(Duration.ofSeconds(60)).until(() ->
                meters.get("fraud.transactions").counter().count() >= processed + 5);
        var factory = context.getBean("&defaultKafkaStreamsBuilder", StreamsBuilderFactoryBean.class);
        var streams = factory.getKafkaStreams();
        factory.stop();
        // Somente o estado temporário deste teste; a recuperação precisa vir do changelog.
        streams.cleanUp();
        factory.start();
        publish("card-recovery", 20, 5, 6);
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(repository.findAll()).singleElement().satisfies(alert -> {
                assertThat(alert.transactionId).isEqualTo(new UUID(20, 5));
                assertThat(alert.rule).isEqualTo(FraudRule.VELOCITY);
            });
        });
    }

    private void publish(String card, long prefix, int start, int end) throws Exception {
        var config = Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all");
        try (var producer = new KafkaProducer<>(config, new StringSerializer(), new JacksonJsonSerializer<Transaction>().noTypeInfo())) {
            for (int i = start; i < end; i++) {
                Transaction transaction = new Transaction(new UUID(prefix, i), card, new BigDecimal("89.97"),
                        "Loja", "Curitiba", -25.43, -49.27, BASE.plusSeconds(i));
                producer.send(new ProducerRecord<>("transactions", card, transaction)).get();
            }
        }
    }

    @Test
    void exposesSwaggerDocumentation() throws Exception {
        mvc.perform(get("/v3/api-docs")).andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/alerts']").exists())
                .andExpect(jsonPath("$.paths['/alerts/{id}/status']").exists());
        mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("fraud_transactions_total")));
        mvc.perform(get("/actuator/env")).andExpect(status().isNotFound());
    }

    private FraudAlert alert(long id, String card, String rule) {
        Transaction transaction = new Transaction(new UUID(0, id), card, new BigDecimal("89.97"),
                "Loja", "Curitiba", -25.43, -49.27, BASE);
        return FraudAlert.create(transaction, rule, Map.of("count", 6), CLOCK);
    }
}
