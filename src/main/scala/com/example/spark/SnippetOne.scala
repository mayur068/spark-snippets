package com.example.spark

import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf


object SnippetOne {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SnippetOne").setMaster("local")
    val sc:SparkContext = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val employee = sqlContext.read.json("resources/input.json")

    print(Constants.state_abbr_map)

    val employee_enriched = employee.withColumn("State_Desc",
      when(col("state") === lit("MH"), "Maharashtra")
        .when(col("state") === lit("DL"), "Delhi")
        .when(col("state") === lit("KA"), "Karnataka")
        .otherwise("NA")
    )

    employee_enriched.show(false)

    sc.stop()

  }
}
