package com.bupt
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class ScalaWordCount {}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    var list = List(
      "hello hi hi spark",
      "hello spark hello hi sparksql",
      "hello hi hi sparkstreaming",
      "hello hi hi sparkgraphx"
    )
    var sparkConf = new SparkConf().setAppName("word-count").setMaster("yarn")
    var sc = new SparkContext(sparkConf)
    var lines: RDD[String] = sc.parallelize(list)
    var words: RDD[String] = lines.flatMap((line: String) => {
      line.split(" ")
    })
    var wordAndOne: RDD[(String, Int)] = words.map((word: String) => {
      (word, 1)
    })
    var wordAndNum: RDD[(String, Int)] =
      wordAndOne.reduceByKey((count1: Int, count2: Int) => { count1 + count2 })
    var ret = wordAndNum.sortBy(kv => kv._2, false)
    print(ret.collect().mkString(","))
    ret.saveAsTextFile("hdfs://serverAddress:8020/spark-test")
    sc.stop
  }
}

// spark-submit --class com.bupt.ScalaWordCount --master yarn --num-executors 3 --driver-memory 1g --executor-memory 1g --executor-cores 1 spark-test.jar
