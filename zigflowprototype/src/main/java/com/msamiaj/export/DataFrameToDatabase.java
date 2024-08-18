package com.msamiaj.export;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class DataFrameToDatabase {
	public static void writeDataToDatabase(Dataset<Row> dataset) {
		String dbConnectionUrl = "jdbc:mysql://localhost:3306/zigflow_prototype";

		Properties prop = new Properties();
		prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
		prop.setProperty("user", "root");
		prop.setProperty("password", "84E4sC2G9g!");

		dataset.write().mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "restaurant", prop);
	}
}
