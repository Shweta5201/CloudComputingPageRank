import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;
import com.google.common.collect.Iterables;

class Sum implements Function2<Double, Double, Double> {
  
    public Double call(Double a, Double b) {
      return a + b;
    }
  }



public class Main {
//C:\Users\Sweta\Downloads\Documents\CloudComputing\hadoop-common-2.2.0-bin-master\hadoop-common-2.2.0-bin-master\bin
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "C:/Users/Sweta/Downloads/Documents/CloudComputing/hadoop-common-2.2.0-bin-master/hadoop-common-2.2.0-bin-master");
		   //String inputFile = "C:/Users/Sweta/workspace/coding/Page_Rank/sample.tsv";
		   String inputFile = args[0];
		   String UniversityFile = args[1];
		  // String outputFile = args[1];
		  final Pattern pattern = Pattern.compile("(?<=<target>)(.*?)(?=</target>)");//("<target>(.+?)</target>");
		   final Pattern pattern1 = Pattern.compile("\\t+");
		  
		
		SparkConf conf = new SparkConf().setAppName("find_links");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile(inputFile);
		//JavaRDD<String> university = sc.textFile(UniversityFile);
		//System.out.println("hello this is count "+input.count());
		HashSet<String> list = new HashSet<String>();
		
		//reading the university file and putting it in hashmap
		BufferedReader reader = new BufferedReader(new FileReader (UniversityFile));
	    String         line = null;
	   // StringBuilder  stringBuilder = new StringBuilder();
	   	  
	        while((line = reader.readLine()) != null) {
	        	String[] temp = line.split(",");
	        	list.add(temp[1]);	
	        	System.out.println("university added :" + temp[1]);
	   	    }
	        reader.close();
	        
	     System.out.println("*******************************************************************total number of universities are :" + list.size());
	   	    
	    final HashSet<String> uniList = new HashSet<String>();
		//***************************************************
		
		
		
		//all target links rdd
		
		JavaPairRDD<String,List<String>> links1 = input.mapToPair(new PairFunction<String,String,List<String>>(){

			public Tuple2<String, List<String>> call(String s) throws Exception {
				// TODO Auto-generated method stub
				String[] components = s.split("\\t");
				//System.out.println("this is component 1 " + components[1]);
				 Matcher matcher = pattern.matcher(s);
		    	  ArrayList<String> link = new ArrayList<String>();
		    	  while(matcher.find()){
		    		  link.add(matcher.group());
		    	  }
		    	//  if(link.size()!=0&& uniList.contains(components[1]))
		    	  if(link.size()!=0)
				return new Tuple2<String,List<String>>(components[1],link);
		    	  else return null;
			}
			
		}).cache();
		
		JavaPairRDD<String,List<String>> links=links1.filter(new Function<Tuple2<String,List<String>>, Boolean>() {

			public Boolean call(Tuple2<String, List<String>> t) throws Exception {
				// TODO Auto-generated method stubs
				
				return t!=null;
			}
			
		}).cache();
		
		
//		JavaPairRDD<String, Iterable<String>> links = input.mapToPair(new PairFunction<String, String, String>() {
//		      public Tuple2<String, String> call(String s) {
//		    //collecting all links value inside target tag
//		    	  System.out.println("line no 48");
//		    	  Matcher matcher = pattern.matcher(s);
//		    	  ArrayList<String> link = new ArrayList<String>();
//		    	  while(matcher.find()){
//		    		  link.add(matcher.group());
//		    	  }
//		    	  
//		    // extracting the title from line
//	         String[] parts = pattern1.split(s);
//	         System.out.println("title = "+parts[1]);
//		        return new Tuple2<String, String>(parts[0], parts[1]);
//		      }
//		    }).distinct().groupByKey().cache();
		 JavaPairRDD<String, Double> ranks = links.mapValues(new Function<List<String>, Double>() {
		  
		      public Double call(List<String> rs) {
		    	/* for(String i : rs) 
		    	  System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^rs^^^^^^^^^^^^^^^^^^^^^^^^^^"+ i);*/
		        return 1.0;
		      }
		    });

		    // Calculates and updates URL ranks continuously using PageRank algorithm.
		    for (int current = 0; current < Integer.parseInt("10"); current++) {
		      // Calculates URL contributions to the rank of other URLs.
		      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
		        .flatMapToPair(new PairFlatMapFunction<Tuple2<List<String>, Double>, String, Double>() {
		        
		          public List<Tuple2<String, Double>> call(Tuple2<List<String>, Double> s) {
		            int urlCount = s._1.size(); //Iterables.size(s._1);
		         //   System.out.println("########## ####### Tuple size is :- "+urlCount);
		            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
		            for (String n : s._1) {
		          //  System.out.println("______________________values added in tuple________________________________"+ n + (s._2() / urlCount));
		              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
		            }
		            return results;
		          }
		      });

		      // Re-calculates URL ranks based on neighbor contributions.
		      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
		      
		        public Double call(Double sum) {
		        //	System.out.println("*************************************************myrank*****"+(0.15 + sum * 0.85));
		          return 0.15 + sum * 0.85;
		        }
		      });
		    }
System.out.println("____________________line 120__________________________________________________________");
		    // Collects all URL ranks and dump them to console.
		
			
// logic to sort the rdd by swapping

//JavaPairRDD<String, String> mappings = input.mapToPair(
//        new PairFunction<String, String, String>() {
//          public Tuple2<String, String> call(String s) {
//            String[] parts = s.split("\t");
//
//            return new Tuple2<String, String>(parts[0], parts[1]);
//          }
//        }).distinct();


System.out.println("****************************number of pages ranked:"+ranks.count());	
JavaPairRDD<String, Double> universityRanks = ranks.filter(new Function<Tuple2<String,Double>,Boolean>(){

	public Boolean call(Tuple2<String, Double> v1) throws Exception {
		// TODO Auto-generated method stub
		if(uniList.contains(v1._1)){
			return true;
		}else
			return false;
	}
	
});


System.out.println("****************************number of universities ranked:"+universityRanks.count());



//----------------------------------------

JavaPairRDD<Double, String> swappedPair = universityRanks.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
    public Tuple2<Double, String> call(Tuple2<String, Double> item) throws Exception {
        return item.swap();
    }
 }).sortByKey(false);






//*****************************************************************************************************************************
			List<Tuple2<Double, String>> output = swappedPair.collect();
		
		    System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%size:" + output.size());
		    PrintWriter f = new PrintWriter(new FileWriter("output.txt"));
		    int count =0;
		    for (Tuple2<?,?> tuple : output) {
		      f.print(tuple._2() + " " + tuple._1() + ".");
		    if(count>100)
		  	  break;
		    }
		f.close();
		    
		    
		    sc.stop();

	}

}
 	