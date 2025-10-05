package com.example.flinkdollarbar.infrastructure.iceberg;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import com.example.flinkdollarbar.domain.entities.DollarBar;

public class DollarBarSink {
    private final StreamTableEnvironment tableEnv;
    private final String warehousePath;
    private final String nessieUri;
    private final String tableName;

    public DollarBarSink(StreamTableEnvironment tableEnv, String warehousePath, String nessieUri, String tableName) {
        this.tableEnv = tableEnv;
        this.warehousePath = warehousePath;
        this.nessieUri = nessieUri;
        this.tableName = tableName;
    }

    public static class DollarBarToRowDataMapper implements MapFunction<DollarBar, RowData>, Serializable {
        @Override
        public RowData map(DollarBar dollarBar) throws Exception {
            GenericRowData rowData = new GenericRowData(9);
            rowData.setField(0, StringData.fromString(dollarBar.getUnderlying()));
            rowData.setField(1, dollarBar.getOpen());
            rowData.setField(2, dollarBar.getHigh());
            rowData.setField(3, dollarBar.getLow());
            rowData.setField(4, dollarBar.getClose());
            rowData.setField(5, dollarBar.getVolume());
            rowData.setField(6, dollarBar.getDollarVolume());
            rowData.setField(7, TimestampData.fromTimestamp(dollarBar.getStartDate()));
            rowData.setField(8, TimestampData.fromTimestamp(dollarBar.getEndDate()));
            return rowData;
        }
    }

    public void createSink(DataStream<DollarBar> dollarBarStream) {
        // Configure Iceberg catalog properties
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("catalog-type", "nessie");
        props.put("uri", nessieUri);
        props.put("ref", "main");
        props.put("nessie.auth.type", "NONE");
        props.put("warehouse", warehousePath);
        props.put("s3.endpoint", "http://myminio-hl.minio-operator:9000");
        props.put("s3.access-key-id", "minioadmin");
        props.put("s3.secret-access-key", "minioadmin");
        props.put("s3.path-style-access", "true");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("catalog-impl", "org.apache.iceberg.nessie.NessieCatalog");

        Configuration hadoopConf = new Configuration();
        // Optionally set S3 configurations in Hadoop conf if needed
        hadoopConf.set("fs.s3a.endpoint", "http://myminio-hl.minio-operator:9000");
        hadoopConf.set("fs.s3a.access.key", "minioadmin");
        hadoopConf.set("fs.s3a.secret.key", "minioadmin");
        hadoopConf.set("fs.s3a.path.style.access", "true");

        CatalogLoader catalogLoader = CatalogLoader.custom(
            "iceberg_catalog",
            props, // Use Map<String, String>
            hadoopConf, // HadoopConf, null if not using Hadoop
            "org.apache.iceberg.nessie.NessieCatalog"
        );

        // Create TableIdentifier
        TableIdentifier tableIdentifier = TableIdentifier.of(tableName);

        Schema schema = new Schema(
            org.apache.iceberg.types.Types.NestedField.required(1, "symbol", org.apache.iceberg.types.Types.StringType.get()),
            org.apache.iceberg.types.Types.NestedField.required(2, "open", org.apache.iceberg.types.Types.DoubleType.get()),
            org.apache.iceberg.types.Types.NestedField.required(3, "high", org.apache.iceberg.types.Types.DoubleType.get()),
            org.apache.iceberg.types.Types.NestedField.required(4, "low", org.apache.iceberg.types.Types.DoubleType.get()),
            org.apache.iceberg.types.Types.NestedField.required(5, "close", org.apache.iceberg.types.Types.DoubleType.get()),
            org.apache.iceberg.types.Types.NestedField.required(6, "volume", org.apache.iceberg.types.Types.DoubleType.get()),
            org.apache.iceberg.types.Types.NestedField.required(7, "dollar_volume", org.apache.iceberg.types.Types.DoubleType.get()),
            org.apache.iceberg.types.Types.NestedField.required(8, "start_timestamp", org.apache.iceberg.types.Types.TimestampType.withoutZone()),
            org.apache.iceberg.types.Types.NestedField.required(9, "end_timestamp", org.apache.iceberg.types.Types.TimestampType.withoutZone())
        );

        // Initialize TableLoader in a serializable way
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        // Create table if it doesn't exist
        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(tableIdentifier)) {
            catalog.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned());
        }

        // Convert DataStream to DataStream<RowData>
        DataStream<RowData> rowDataStream = dollarBarStream.map(new DollarBarToRowDataMapper());
        
        FlinkSink.forRowData(rowDataStream)
            .tableLoader(tableLoader)
            .append();
    }
}