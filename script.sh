# (optional) if your input is under your home dir:
# hdfs dfs -ls /user/$USER/dataset

# remove any old outputs
#hdfs dfs -rm -r -f /output/dataset1_stage2 /output/dataset1

# run the 3-stage driver
#hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/DocumentSimilarity-0.0.1-SNAPSHOT.jar \
#  com.example.controller.DocumentSimilarityDriver \
# /user/$USER/dataset/dataset_1_1000_words.txt \
#  /output/dataset1_stage2 \
#  /output/dataset1


mvn clean package -DskipTests
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
docker cp script2.sh resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/script2.sh

