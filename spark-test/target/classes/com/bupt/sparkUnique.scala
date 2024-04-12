package com.bupt
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class sparkUnique {
  
}

object sparkUnique {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("word-unique").setMaster("yarn")
    var sc = new SparkContext(sparkConf)
    var A=sc.textFile("A.txt")
    var B=sc.textFile("B.txt")
    var C=A.union(B).distinct()
    C.saveAsTextFile("hdfs://serverAddress:8020/ans")
  }
}

// spark-submit --class com.bupt.sparkUnique --master yarn --num-executors 3 --driver-memory 1g --executor-memory 1g --executor-cores 1 spark-test-unique.jar