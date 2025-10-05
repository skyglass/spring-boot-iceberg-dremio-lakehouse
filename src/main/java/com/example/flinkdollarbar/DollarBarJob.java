package com.example.flinkdollarbar;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.example.flinkdollarbar.domain.entities.DollarBar;
import com.example.flinkdollarbar.domain.entities.PriceAction;
import com.example.flinkdollarbar.infrastructure.iceberg.DollarBarSink;
import com.example.flinkdollarbar.infrastructure.kafka.PriceActionSource;
import com.example.flinkdollarbar.usecases.ProcessPriceActionUseCase;

public class DollarBarJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("log.level", "DEBUG");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Configuration (replace with your values)
        String kafkaBootstrapServers = "application-kafka-bootstrap.kafka:9092";
        String kafkaTopic = "price-action";
        String warehousePath = "s3://warehouse";
        String nessieUri = "http://nessie.nessie:19120/api/v2";
        String icebergTableName = "dollar_bars";
        double dollarThreshold = 10000.0;

        // Create Kafka source
        PriceActionSource source = new PriceActionSource(env, kafkaBootstrapServers, kafkaTopic);
        DataStream<PriceAction> priceActionStream = source.createSource();

        // Process price actions to dollar bars
        ProcessPriceActionUseCase useCase = new ProcessPriceActionUseCase(dollarThreshold);
        DataStream<DollarBar> dollarBarStream = useCase.execute(priceActionStream);

        DollarBarSink sink = new DollarBarSink(tableEnv, warehousePath, nessieUri, icebergTableName);
        sink.createSink(dollarBarStream);

        // Execute the job
        env.execute("Dollar Bar Calculation Job");
    }
}