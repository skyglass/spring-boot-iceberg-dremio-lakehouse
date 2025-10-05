package com.example.flinkdollarbar.infrastructure.kafka;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.flinkdollarbar.domain.entities.PriceAction;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PriceActionSource {
    private static final Logger LOG = LoggerFactory.getLogger(PriceActionSource.class);

    private final StreamExecutionEnvironment env;
    private final String bootstrapServers;
    private final String topic;

    public PriceActionSource(StreamExecutionEnvironment env, String bootstrapServers, String topic) {
        this.env = env;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    public DataStream<PriceAction> createSource() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", "flink-dollar-bar-consumer");

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId("flink-dollar-bar-consumer")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map(new PriceActionMapper())
            .name("parse-price-action")
            .returns(PriceAction.class);
    }

    private static class PriceActionMapper extends RichMapFunction<String, PriceAction> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static final DateTimeFormatter TIMESTAMP_FORMATTER = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSS")
            .withZone(ZoneId.of("UTC"));
        
        @Override
        public PriceAction map(String json) throws Exception {
            try {
                JsonPriceAction jsonPriceAction = OBJECT_MAPPER.readValue(json, JsonPriceAction.class);
                if (jsonPriceAction.date == null) {
                    throw new IllegalArgumentException("Date is null in JSON: " + json);
                }
                // Parse the timestamp with proper handling
                Instant instant = parseTimestamp(jsonPriceAction.date);
                return new PriceAction(
                    jsonPriceAction.low,
                    jsonPriceAction.high,
                    jsonPriceAction.open,
                    jsonPriceAction.close,
                    jsonPriceAction.volume,
                    jsonPriceAction.underlying,
                    Timestamp.from(instant)
                );
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: {}", json, e);
                throw e;
            }
        }

        private Instant parseTimestamp(String dateString) {
            try {
                // Try parsing as ISO instant first (with Z or timezone)
                return Instant.parse(dateString);
            } catch (DateTimeParseException e1) {
                try {
                    // Try parsing as local datetime and assume UTC
                    LocalDateTime localDateTime = LocalDateTime.parse(dateString);
                    return localDateTime.atZone(ZoneId.of("UTC")).toInstant();
                } catch (DateTimeParseException e2) {
                    try {
                        // Try with custom formatter for your specific format
                        return Instant.from(TIMESTAMP_FORMATTER.parse(dateString));
                    } catch (DateTimeParseException e3) {
                        throw new IllegalArgumentException(
                            "Unable to parse timestamp: " + dateString + 
                            ". Expected formats: ISO-8601 with timezone, yyyy-MM-dd'T'HH:mm:ss, or yyyy-MM-dd'T'HH:mm:ss.SSSSS", 
                            e3);
                    }
                }
            }
        }
    }

    private static class JsonPriceAction {
        public double low;
        public double high;
        public double open;
        public double close;
        public double volume;
        public String underlying;
        public String date;
    }
}