package com.bupt
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext,SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import java.util.Properties

class InsertStudent {}

object InsertStudent {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setAppName("insert-student").setMaster("local")
    val sc = new SparkContext(sparkConf)
    // 学生信息RDD
    val studentRDD = sc
      .parallelize(Array("3 Zhang M 26", "4 Liu M 27"))
      .map(_.split(" "))
    // 模式信息
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField ("name", StringType, true),
        StructField ("gender", StringType, true),
        StructField ("age", IntegerType, true)
      )
    )
    // Row对象
    val rowRDD =
      studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    // 建立起Row对象和模式之间的关系
    val studentDF = new SQLContext(sc).createDataFrame(rowRDD, schema)
    // JDBC连接参数
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "PassW0Rd!")
    prop.put("driver", "com.mysql.jdbc.Driver")
    // 连接数据库，append
    studentDF.write
      .mode("append")
      .jdbc(
        "jdbc:mysql://localhost:3306/spark",
        "spark.student",
        prop
      )
  }
}

// spark-submit --class com.bupt.InsertStudent --master yarn --num-executors 3 --driver-memory 1g --executor-memory 1g --executor-cores 1 spark-test-mysql.jar

