package com.risksense;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;



import scala.Tuple2;


public class ReadParquetData implements Serializable {

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
		
		Configuration config = null;
		try {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", args[0]);
			config.set("hbase.zookeeper.property.clientPort", "2181");
			config.set("hbase.master", args[1]);
			
			config.set("hbase.client.keyvalue.maxsize","0");
			HBaseAdmin.checkHBaseAvailable(config);
			System.out.println("HBase is running!");
		} catch (MasterNotRunningException e) {
			System.out.println("HBase is not running!");
			System.exit(1);
		} catch (Exception ce) {
			ce.printStackTrace();
		}
		
		Job newAPIJobConfiguration1 = Job.getInstance(config);

		generateSeverityLevelIps(newAPIJobConfiguration1,spark,args[2]);
		generatePortLevelIps(newAPIJobConfiguration1,spark,args[2]);
		generateNoOfRecords(newAPIJobConfiguration1,spark,args[2]);
		generatecveIps(newAPIJobConfiguration1,spark,jsc,args[2]);

	}

	private static void generatecveIps(Job newAPIJobConfiguration1, SparkSession spark,JavaSparkContext jsc,String combinedDataPath) {
		
		Dataset<Row> ipsForCVEID = spark.read().parquet(combinedDataPath).select("VULN.CVE_LIST.CVE","ip").filter("VULN.CVE_LIST.CVE is not null");
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "cve_ips");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		JavaRDD<List<Tuple2<String, String>>> cveIpPair = ipsForCVEID.toJavaRDD().map(new Function<Row, List<Tuple2<String, String>>>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public List<Tuple2<String, String>> call(Row row) throws Exception {

						List<Tuple2<String,String>> cveList = new ArrayList<>();
						JsonElement jelement = null;
						if (row.getString(0) != null) {
							jelement = new JsonParser().parse(row.getString(0));
							if (jelement.getClass().isInstance(new JsonArray())) {
								JsonArray jarray = jelement.getAsJsonArray();

								for (JsonElement js : jarray) {
									JsonObject jobject = js.getAsJsonObject();
									cveList.add(new Tuple2<String, String>(jobject.get("ID").getAsString(),row.getString(1)));
								}
								return  cveList;
							} else {
								JsonObject jobject = jelement.getAsJsonObject();
								cveList.add(new Tuple2<String, String>(jobject.get("ID").getAsString(),row.getString(1)));
							}
							return cveList;
						}
						return new ArrayList<Tuple2<String, String>>();

					}
				});
		
		List<Tuple2<String, String>> bcd = new ArrayList<>();
		
		for(List<Tuple2<String, String>> a : cveIpPair.collect()) {
			Iterator<Tuple2<String, String>> itr = a.iterator();
			while(itr.hasNext()) {
				Tuple2<String, String> str =  itr.next();
				bcd.add(new Tuple2<String,String>(str._1,str._2));
			}
		}

		JavaPairRDD<String, String> cveIpPairRdd = jsc.parallelizePairs(bcd);

		JavaPairRDD<ImmutableBytesWritable, Put> cvePuts = cveIpPairRdd.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, ImmutableBytesWritable, Put>() {
			
			private static final long serialVersionUID = 1L;

					@SuppressWarnings("deprecation")
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Iterable<String>> t)	throws Exception {
						
						Put put = null;
						if(!t._1.isEmpty()) {
							put = new Put(Bytes.toBytes(t._1));
							put.add(Bytes.toBytes("cve_fmly"), Bytes.toBytes("ips"), Bytes.toBytes(t._2.toString()));
							return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
						}

						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}

				});
		
		cvePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
	}

	private static void generateNoOfRecords(Job newAPIJobConfiguration1, SparkSession spark,String combinedDataPath) {

		Dataset<Row> noOfRecsForIps = spark.read().parquet(combinedDataPath).groupBy("ip").agg(org.apache.spark.sql.functions.count(org.apache.spark.sql.functions.lit(1)).as("count"));
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "no_of_records");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		
		JavaPairRDD<ImmutableBytesWritable, Put> recordPuts = noOfRecsForIps.toJavaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@SuppressWarnings("deprecation")
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {

						
						Put put = new Put(Bytes.toBytes(row.getString(0)));
						put.add(Bytes.toBytes("nor_cf"), Bytes.toBytes("no_of_records"),Bytes.toBytes(Long.toString(row.getLong(1))));
						
						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}
				});
		

		recordPuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		
	}

	private static void generatePortLevelIps(Job newAPIJobConfiguration1, SparkSession spark,String combinedDataPath) {
		
		Dataset<Row> ipsForPorts = spark.read().parquet(combinedDataPath).groupBy("port").agg(org.apache.spark.sql.functions.collect_list("ip"));
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "port_level_ips");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		
		JavaPairRDD<ImmutableBytesWritable, Put> portPuts = ipsForPorts.toJavaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@SuppressWarnings("deprecation")
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {

						
						Put put = new Put(Bytes.toBytes(row.getString(0)));						
						put.add(Bytes.toBytes("port_cf"), Bytes.toBytes("ips"),Bytes.toBytes(row.getList(1).toString()));
						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}
				});
		

		portPuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());		
	}

	private static void generateSeverityLevelIps(Job newAPIJobConfiguration1,SparkSession spark,String combinedDataPath) {
		
		Dataset<Row> ipsForDate = spark.read().parquet(combinedDataPath).groupBy("dateloaded.$date", "VULN.SEVERITY_LEVEL").agg(org.apache.spark.sql.functions.collect_list("ip"));
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "severity_level_ips");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

		JavaPairRDD<ImmutableBytesWritable, Put> datePuts = ipsForDate.toJavaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@SuppressWarnings("deprecation")
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {

						List<?> ls = row.getList(2);
						Put put = new Put(Bytes.toBytes(row.getString(0)));
						put.add(Bytes.toBytes("severity_level"), Bytes.toBytes("level"),Bytes.toBytes(Long.toString(row.getLong(1))));
						put.add(Bytes.toBytes("severity_level"), Bytes.toBytes("noOfIps"),Bytes.toBytes(Integer.toString(ls.size())));
						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}
				});

		datePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		
	}

}
