# Pagerank-using-Mapreduce-Hadoop
javac -classpath ${HADOOP_CLASSPATH} -d PageRank/ *.java */*.java
jar -cvf pagerank.jar -C PageRank/ .
hdfs dfs -copyFromLocal <location_of_data> /user/ABC/stanford.txt
hadoop jar pagerank.jar com.pagerank.PageRank --input /user/ABC/stanford.txt --output /user/ABC/output2
