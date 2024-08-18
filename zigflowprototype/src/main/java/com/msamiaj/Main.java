package com.msamiaj;

import org.apache.spark.sql.SparkSession;

import com.msamiaj.export.DataFrameToDatabase;
import com.msamiaj.ingestion.Ingestion;

public class Main {
    private SparkSession sparkSession;

    public static void main(String[] args) {
        String exportFlag = "dontexport";
        if (args.length != 0)
            exportFlag = args[0];
        new Main().startApplication(exportFlag);
    }

    private void startApplication(String exportFlag) {
        sparkSession = SparkSession.builder().appName("zigflowprototype")
                .master("local").getOrCreate();

        var ingestion = new Ingestion(sparkSession);
        ingestion.performIngestionOfDatasets().processDataSets();

        if (exportFlag.compareToIgnoreCase("exportdata") == 0) {
            DataFrameToDatabase.writeDataToDatabase(ingestion.getUnionDataset());
        }
    }
}
