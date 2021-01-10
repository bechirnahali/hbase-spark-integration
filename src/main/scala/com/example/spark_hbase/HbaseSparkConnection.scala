package com.example.spark_hbase

import org.apache.spark.sql._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration

object HbaseSparkConnection {
  def main(args: Array[String]): Unit = {

    //Define spark session

    val spark = SparkSession.builder().getOrCreate()

    // Hbase connection setup with help of zookeeper:

    val conf = new HBaseConfiguration()
    conf.set("hbase.zookeeper.quorum", "hostname1,hostname2...") //zookeeper servers
    conf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper server port
    new HBaseContext(spark.sparkContext, conf)

    //Read data from Hbase into a Spark Dataframe:

    val sql = spark.sqlContext

    val hbaseTable = "default:WAR_PLAN" // Hbase table name

    val columnMapping =
      """id string :key,
        |infantryNumber string: infantry:number,
        |cavalryNumber string: cavalry:number""".stripMargin //Mapping between Hbase table and Spark dataframe

    val hbaseSource = "org.apache.hadoop.hbase.spark" // Library responsible for fetching hbase data into spark

    val hbaseData = sql.read.format(hbaseSource).option("hbase.columns.mapping", columnMapping).option("hbase.table", hbaseTable)
    val hbaseDf= hbaseData.load() //Load data into a Dataframe
    hbaseDf.createOrReplaceTempView("hbaseDataframe") // Save the dataframe into a temp view for sql querying


    hbaseDf.show() // Check dataframe content
    hbaseDf.printSchema // Check dataframe mapped columns

    //Write data from Spark dataframe into an hbase table :

    val hiveTmp = spark.sql("select * from default.war_plan") // Select data from Hive table

    val columns: Array[String]= hbaseDf.columns
    val hiveDf = hiveTmp.select(columns.head, columns.tail: _*) // Select hive dataframe columns in the same order as those for hbase.
    hiveDf.createOrReplaceTempView("hiveDataframe")


    val insertStatement = "insert into hiveDataframe select * from hbaseDataframe"
    spark.sql(insertStatement) // Execute the insert statement.




  }
}
