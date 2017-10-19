Page rank execution:

step1:
to parse the file and remove dangling links use parser.java
command java -jar parser.jar inputwikidata
no need to give output. generates output.txt

step 2:
use the output file generated in Pagerank.scala
command:  spark-submit --class "Pagerank" --master local[2] jarname inputpath outputpath

Graphx:
for graphX directly input the wikiex data set 

For university same as pagerank:
the university rank is calclulated by the contribution factor. basically a university is ranked higher if it has the hightest contribution factor.
for mapping university i have downloaded a list of university and basically code calclutates the rank for these universities.
command:  spark-submit --class "Pagerank" --master local[2] jarname inputpath listofuniversity





Setting up EC2 Cluster:
Namenode : ec2-54-211-31-67.compute-1.amazonaws.com





1.	Parsing the file in the links->target
Using java, I parsed the file and removed the dangling links. The file generated is called output.txt. The syntax to run this parser is

Java -jar jarfile inputfile(wiki articles data) 
The out-put is automatically written in output.txt

2.	Running page rank on local and cluster:

Commands used to execute files on local and cluster

spark-submit --class "Main" --master local[2] "C:\Users\Sweta\workspace\coding\University_Rank\target\University_Rank-0.0.1-SNAPSHOT.jar" "C:\Users\Sweta\workspace\coding\Page_Rank\sample.tsv" "C:\Users\Sweta\workspace\coding\University_Rank\world-universities.csv"

spark-submit2 --class "Main" --executor-memory 10g --driver-memory 10g --master local[2] "C:\Users\Sweta\workspace\coding\Assignment2\target\PG-1.0-SNAPSHOT.jar" "C:\Users\Sweta\Downloads\Documents\CloudComputing\newoutput.txt" 

spark-submit --class "Main" --master local[2] "C:\Users\Sweta\workspace\coding\Page_Rank\target\University_Rank-0.0.1-SNAPSHOT.jar" "C:\Users\Sweta\workspace\coding\Page_Rank\sample.tsv"

./spark/bin/spark-submit --class "Main" --executor-memory 10g --driver-memory 2g --master spark://ec2-34-231-244-15.compute-1.amazonaws.com:7077 Assignment2/Page_Rank-0.0.1-SNAPSHOT.jar /ec2-34-231-244-15.compute-1.amazonaws.com:7077/user/root/Assignment2/sample.tsv



./spark/bin/spark-submit --class "Main" --executor-memory 1g --driver-memory 1g --master spark://ec2-34-231-244-15.compute-1.amazonaws.com:7077 Assignment2/Page_Rank-0.0.1-SNAPSHOT.jar file://user/root/Assignment2/sample.tsv

spark/bin/spark-submit --class "Main" --executor-memory 1g --driver-memory 1g --master spark://ec2-34-231-244-15.compute-1.amazonaws.com:7077 Assignment2/Page_Rank-0.0.1-SNAPSHOT.jar output.txt


3.	comparison between graphx and spark x:
for the total job spark took approximately 11minutes on my cluster of 10 machines 
on the other hand graph though initially slow picked up in later iteration and took approximately 7minutes.
1. GraphX takes more time than Spark to load the graph, because it has to index it, but subsequent
iterations should be faster

2. we found that the optimized Spark PageRank was only 3x slower than
GraphX.

4.	For comparison of university :
I scraped the name of university in a file and then while calculating the page rank I compared it with the titles. After page ranking I used only those page ranks which are related to universities.
And then extract the top 100 university.

