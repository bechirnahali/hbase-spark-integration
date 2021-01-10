package com.example.spark_hbase

import org.apache.spark.sql._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration

object HbaseSparkConnection {
  def main(args: Array[String]): Unit = {

    //Connection Establishment

    val spark = SparkSession.builder().getOrCreate()

    val conf = new HBaseConfiguration()
    conf.set("hbase.zookeeper.quorum", "master2.orange.com,master1.orange.com,utility.orange.com")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    new HBaseContext(spark.sparkContext, conf)

    //Read data

    val sql = spark.sqlContext

    Var HBase table 

    Var column mapping (well structured)

    Var source

    val data = sql.read.format("org.apache.hadoop.hbase.spark").option("hbase.columns.mapping", mapping_hbase).option("hbase.table", hbase_table)
    val df= data.load()
    df.createOrReplaceTempView("name")


    df.show()
    df.printSchema

    //Write data

    val hiveData = spark.sql("select * from default.sampleTable")
    hiveData.createOrReplaceTempView("name")

    val insertStatement

    spark.sql(insertStatement)




  }
}
