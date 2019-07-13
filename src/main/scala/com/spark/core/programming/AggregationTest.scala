package com.spark.core.programming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object AggregationTest {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Test")
    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    import session.implicits._

    val rawRdd: RDD[String] = sc.textFile("src/main/resources/data.csv")
    val convertedRdd: RDD[RawValues] = rawRdd.map(x => x.split(","))
      .map(x => RawValues(x(0), x(1)))
    var dataDF: DataFrame = convertedRdd.toDF()

    // If we want to store the data in the parquet format which is mentioned in the document
    dataDF.write.mode(SaveMode.Overwrite).partitionBy("country").parquet("src/main/resources/input/")

    // If we want read the above saved parquet file into our application
    dataDF = session.read.parquet("src/main/resources/input/")

    val aggregate = new AggregationUDAF

    val aggregateDF = dataDF.groupBy($"country").agg(aggregate($"values").as("values"))
    aggregateDF.write.mode(SaveMode.Append).parquet("src/main/resources/output/")

    sc.stop()
  }
}
