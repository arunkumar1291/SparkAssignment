package com.risksense;

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


public class CombineData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("Combine Data");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		SparkSession spark = sqlContext.sparkSession();

		Dataset<Row> assetdf = sqlContext.read().json(jsc.wholeTextFiles(args[0]).values()).toDF();
		
		Dataset<Row> vulndf = spark.read().json(args[1]);

		Dataset<?> ds = assetdf.join(vulndf, assetdf.col("qid").equalTo(vulndf.col("VULN.QID"))).select("*");
		
		ds.write().parquet(args[2]);
	}

}
