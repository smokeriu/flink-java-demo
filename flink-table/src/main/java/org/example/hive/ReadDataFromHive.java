package org.example.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class ReadDataFromHive {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String catalogName = "myhive";
        String defaultDatabase = "flink_data";


        HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, null);
        tableEnv.registerCatalog(catalogName, hive);

        tableEnv.useCatalog(catalogName);

        String sourceTable = "test";
        final Table first_data = tableEnv.from(sourceTable);
        first_data.execute().print();
        first_data.executeInsert(sourceTable);
    }
}
