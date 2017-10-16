
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//
//
//import scala.Tuple2;
//
//import com.google.common.collect.Iterables;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.regex.Pattern;
//
///**
// * Computes the PageRank of URLs from an input file. Input file should
// * be in format of:
// * URL         neighbor URL
// * URL         neighbor URL
// * URL         neighbor URL
// * ...
// * where URL and their neighbors are separated by space(s).
// *
// * This is an example implementation for learning how to use Spark. For more conventional use,
// * please refer to org.apache.spark.graphx.lib.PageRank
// */
//public final class Main {
//    private static final Pattern SPACES = Pattern.compile("\\s+");
//    
//
//    static void showWarning() {
//        String warning = "WARN: This is a naive implementation of PageRank " +
//                "and is given as an example! \n" +
//                "Please use the PageRank implementation found in " +
//                "org.apache.spark.graphx.lib.PageRank for more conventional use.";
//        System.err.println(warning);
//    }
//
//    private static class Sum implements Function2<Double, Double, Double> {
//       
//        public Double call(Double a, Double b) {
//            return a + b;
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//    	System.setProperty("hadoop.home.dir", "C:/Users/Sweta/Downloads/Documents/CloudComputing/hadoop-common-2.2.0-bin-master/hadoop-common-2.2.0-bin-master");
//		
//
//        showWarning();
//
//        SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank");
//        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//
//        // Loads in input file. It should be in format of:
//        //     URL         neighbor URL
//        //     URL         neighbor URL
//        //     URL         neighbor URL
//        //     ...
//        JavaRDD<String> lines = ctx.textFile(args[0], 1);
//
//        // Loads all URLs from input file and initialize their neighbors.
//        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
//        
//            public Tuple2<String, String> call(String s) {
//                String[] parts = SPACES.split(s);
//                return new Tuple2<String, String>(parts[0], parts[1]);
//            }
//        }).distinct().groupByKey().cache();
//
//        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
//        JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
//           
//            public Double call(Iterable<String> rs) {
//                return 1.0;
//            }
//        });
//
//        // Calculates and updates URL ranks continuously using PageRank algorithm.
//        for (int current = 0; current < Integer.parseInt("10"); current++) {
//            // Calculates URL contributions to the rank of other URLs.
//            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
//                    .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
//                      
//                        public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
//                            int urlCount = Iterables.size(s._1);
//                            Iterator<Tuple2<String, Double>> results = new Iterator<Tuple2<String, Double>>();
//                            for (String n : s._1) {
//                                results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
//                            }
//                            return results;
//                        }
//
//						
//                    });
//
//            // Re-calculates URL ranks based on neighbor contributions.
//            ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
//             
//                public Double call(Double sum) {
//                    return 0.15 + sum * 0.85;
//                }
//            });
//        }
//
//        // Collects all URL ranks and dump them to console.
//        List<Tuple2<String, Double>> output = ranks.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
//        }
//
//        ctx.stop();
//    }
//}





/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Iterables;

import scala.Tuple2;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * <pre>
 * bin/run-example JavaPageRank data/mllib/pagerank_data.txt 10
 * </pre>
 */
public final class Main {
  private static final Pattern SPACES = Pattern.compile("\\s+");

  static void showWarning() {
    String warning = "WARN: This is a naive implementation of PageRank " +
            "and is given as an example! \n" +
            "Please use the PageRank implementation found in " +
            "org.apache.spark.graphx.lib.PageRank for more conventional use.";
    System.err.println(warning);
  }

  private static class Sum implements Function2<Double, Double, Double> {
   
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {
   
	  System.setProperty("hadoop.home.dir", "C:/Users/Sweta/Downloads/Documents/CloudComputing/hadoop-common-2.2.0-bin-master/hadoop-common-2.2.0-bin-master");
    showWarning();
    String inputFile = args[0];
    SparkConf conf = new SparkConf().setAppName("find_links");
	JavaSparkContext sc = new JavaSparkContext(conf);
	JavaRDD<String> lines = sc.textFile(inputFile);

//    SparkSession spark = SparkSession
//      .builder()
//      .appName("JavaPageRank")
//      .getOrCreate();

    // Loads in input file. It should be in format of:
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     ...
    //JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    // Loads all URLs from input file and initialize their neighbors.
    JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
      String[] parts = SPACES.split(s);
      return new Tuple2<>(parts[0], parts[1]);
    }).distinct().groupByKey().cache();

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < Integer.parseInt("2"); current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair(s -> {
          int urlCount = Iterables.size(s._1());
          List<Tuple2<String, Double>> results = new ArrayList<>();
          for (String n : s._1) {
            results.add(new Tuple2<>(n, s._2() / urlCount));
          }
          return results.iterator();
        });

      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
    }

    // Collects all URL ranks and dump them to console.
    JavaPairRDD<Double, String> swappedPair = ranks.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
        public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
            return item.swap();
        }
     }).sortByKey(false);
    
    System.out.println("***************************************************************************************************************");
    
    List<Tuple2<Double, String>> output = swappedPair.collect();
//    output.sort(new Comparator<Tuple2<String,Double>>() {
//
//		@Override
//		public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
//			// TODO Auto-generated method stub
//			
//			return (int) (o2._2-o1._2);
//		}
//	});
    PrintWriter f = new PrintWriter(new FileWriter("output.txt"));
    int count =0;
    for (Tuple2<?,?> tuple : output) {
      f.print(tuple._2() + " " + tuple._1() + ".");
    if(count>100)
  	  break;
    }
f.close();

  }
}
