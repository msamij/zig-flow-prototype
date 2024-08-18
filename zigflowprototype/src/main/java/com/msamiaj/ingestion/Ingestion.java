package com.msamiaj.ingestion;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.to_timestamp;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ingestion {
	private final SparkSession sparkSession;
	private Dataset<Row> wakeRestaurantsDataFrame;
	private Dataset<Row> durhamRestaurantsDataFrame;
	private Dataset<Row> unionDataFrame;

	public Ingestion(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/**
	 * Perform ingestion of Durham Restaurants and Wake County Restaurants datasets.
	 * 
	 * @return Ingestion
	 */
	public Ingestion performIngestionOfDatasets() {
		ingestWakeRestaurantsDataFrame();
		ingestDurhamRestaurantsDataFrame();
		return this;
	}

	/**
	 * Perform processing of datasets and builds a larger dataset by taking union.
	 * 
	 * @return the dataframe which contains the union of the two datasets.
	 */
	public void processDataSets() {
		var buildDataFrame = new ProcessDataSets();
		Dataset<Row> wakeRestaurantsDataFrame = buildDataFrame.buildWakeRestaurantsDataframe();
		Dataset<Row> durhamRestaurantsDataFrame = buildDataFrame.buildDurhamRestaurantsDataframe();
		buildDataFrame.performUnion(wakeRestaurantsDataFrame, durhamRestaurantsDataFrame);
		buildDataFrame.summarizeData();
	}

	private void ingestWakeRestaurantsDataFrame() {
		this.wakeRestaurantsDataFrame = this.sparkSession.read()
				.format("csv")
				.option("header", "true")
				.load("data/Restaurants_in_Wake_County_NC.csv");

	}

	private void ingestDurhamRestaurantsDataFrame() {
		this.durhamRestaurantsDataFrame = this.sparkSession.read()
				.format("json")
				.option("multiline", true)
				.load("data/Restaurants_in_Durham_County_NC.json");

	}

	private class ProcessDataSets {

		/**
		 * Performs the union between the two dataframes to build a larger dataset.
		 * 
		 * @param df1 Dataframe to union on.
		 * @param df2 Dataframe to union from.
		 */
		private void performUnion(Dataset<Row> df1, Dataset<Row> df2) {
			unionDataFrame = df1.unionByName(df2);

			unionDataFrame = unionDataFrame.withColumn("start_date",
					date_format(to_timestamp(unionDataFrame.col("dateStart"),
							"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
							"dd-MM-yyyy"))
					.drop("dateStart");

			unionDataFrame = unionDataFrame.withColumn("end_date",
					date_format(to_timestamp(unionDataFrame.col("dateEnd"),
							"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
							"dd-MM-yyyy"))
					.drop("dateEnd");

			unionDataFrame.show(10);

			// Prints the schema of the dataset.
			unionDataFrame.printSchema();
			System.out.println("We have " + unionDataFrame.count() + " records.");

			// Current no of partitions we have running.
			Partition[] partitions = unionDataFrame.rdd().partitions();
			int partitionCount = partitions.length;
			System.out.println("Partition count: " + partitionCount);
		}

		/**
		 * Summarizes the data by performing aggregation functions such as:
		 * </p>
		 * avg, min and max on geoX, geoY and date columns.
		 */
		private void summarizeData() {
			Dataset<Row> summary = unionDataFrame.agg(
					avg("geoX").alias("geoX_avg"),
					min("geoX").alias("geoX_min"),
					max("geoX").alias("geoX_max"),

					avg("geoY").alias("geoY_avg"),
					min("geoY").alias("geoY_min"),
					max("geoY").alias("geoY_max"),

					min("start_date").alias("min_date"),
					max("start_date").alias("max_date"));

			System.out.println("**************Summarized data**************");
			summary.show(10);
		}

		private Dataset<Row> buildWakeRestaurantsDataframe() {
			wakeRestaurantsDataFrame = wakeRestaurantsDataFrame.withColumn("county", lit("Wake"))
					.withColumnRenamed("HSISID", "datasetId")
					.withColumnRenamed("NAME", "name")
					.withColumnRenamed("ADDRESS1", "address1")
					.withColumnRenamed("ADDRESS2", "address2")
					.withColumnRenamed("CITY", "city")
					.withColumnRenamed("STATE", "state")
					.withColumnRenamed("POSTALCODE", "zip")
					.withColumnRenamed("PHONENUMBER", "tel")
					.withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
					.withColumn("dateEnd", lit(null))
					.withColumnRenamed("FACILITYTYPE", "type")
					.withColumnRenamed("X", "geoX")
					.withColumnRenamed("Y", "geoY")
					.drop(wakeRestaurantsDataFrame.col("OBJECTID"))
					.drop(wakeRestaurantsDataFrame.col("GEOCODESTATUS"))
					.drop(wakeRestaurantsDataFrame.col("PERMITID"));

			wakeRestaurantsDataFrame = wakeRestaurantsDataFrame.withColumn("id",
					concat(wakeRestaurantsDataFrame.col("state"), lit("_"),
							wakeRestaurantsDataFrame.col("county"), lit("_"),
							wakeRestaurantsDataFrame.col("datasetId")));

			return wakeRestaurantsDataFrame;
		}

		private Dataset<Row> buildDurhamRestaurantsDataframe() {
			durhamRestaurantsDataFrame = durhamRestaurantsDataFrame.withColumn("county", lit("Durham"))
					.withColumn("datasetId", durhamRestaurantsDataFrame.col("fields.id"))
					.withColumn("name", durhamRestaurantsDataFrame.col("fields.premise_name"))
					.withColumn("address1",
							durhamRestaurantsDataFrame.col("fields.premise_address1"))
					.withColumn("address2",
							durhamRestaurantsDataFrame.col("fields.premise_address2"))
					.withColumn("city", durhamRestaurantsDataFrame.col("fields.premise_city"))
					.withColumn("state", durhamRestaurantsDataFrame.col("fields.premise_state"))
					.withColumn("zip", durhamRestaurantsDataFrame.col("fields.premise_zip"))
					.withColumn("tel", durhamRestaurantsDataFrame.col("fields.premise_phone"))
					.withColumn("dateStart", durhamRestaurantsDataFrame.col("fields.opening_date"))
					.withColumn("dateEnd", durhamRestaurantsDataFrame.col("fields.closing_date"))
					.withColumn("type",
							split(durhamRestaurantsDataFrame.col("fields.type_description"),
									" - ").getItem(1))
					.withColumn("geoX", durhamRestaurantsDataFrame.col("fields.geolocation")
							.getItem(0))
					.withColumn("geoY", durhamRestaurantsDataFrame.col("fields.geolocation")
							.getItem(1))
					.drop(durhamRestaurantsDataFrame.col("fields"))
					.drop(durhamRestaurantsDataFrame.col("geometry"))
					.drop(durhamRestaurantsDataFrame.col("record_timestamp"))
					.drop(durhamRestaurantsDataFrame.col("recordid"));

			durhamRestaurantsDataFrame = durhamRestaurantsDataFrame.withColumn("id",
					concat(durhamRestaurantsDataFrame.col("state"), lit("_"),
							durhamRestaurantsDataFrame.col("county"), lit("_"),
							durhamRestaurantsDataFrame.col("datasetId")));

			return durhamRestaurantsDataFrame;
		}
	}

	public Dataset<Row> getUnionDataset() {
		return unionDataFrame;
	}
}
