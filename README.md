# LSH
Usage:
1. build a project https://azure.microsoft.com/en-us/documentation/articles/hdinsight-develop-deploy-java-mapreduce-linux/
2. put PageRank.java under src/main/java/org/apache/hadoop/examples/PageRank.java
3. run "mvn" under LSH root directory
4. yarn jar target/pagerankjava-1.0-SNAPSHOT.jar org.apache.hadoop.examples.PageRank [inputfile path] [output directory path] 
