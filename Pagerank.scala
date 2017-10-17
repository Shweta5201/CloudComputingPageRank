package com.devinline.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.xml.{ XML, NodeSeq }



object Pagerank {
    System.setProperty("hadoop.home.dir", "C:/winutils")
   def main(args: Array[String]) {
    //Taking command line arguments to run the correct config 
    //1. Input HDFS File
    //2. Output HDFS File
    //3. Number of iterations (optional)
    val inputHadoopFile = args(0)
    val outputHadoopFile = args(1)
    val iterations = if (args.length > 2) args(2).toInt else 10
    val config = new SparkConf()
    config.setAppName("SparkPageRank")
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    config.setMaster("local[10]").set("spark.executor.memory","3g")
    val context = new SparkContext(config)
    val lines = context.textFile(inputHadoopFile)
   
  
    // Arranging the pages into graph structure to implement PageRank
   val iters = 10
    
    val links = lines.map{ s =>
      val parts = s.split("#")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

   val output = ranks.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
   context.parallelize(output).saveAsTextFile(outputHadoopFile)
    ranks.collect().foreach(tup => println(tup._1 + "    " + tup._2 + "\n"))
      context.stop()
    
  }
}