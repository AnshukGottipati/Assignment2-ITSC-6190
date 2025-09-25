# Assignment 2: Document Similarity using MapReduce

**Name: Anshuk Gottipati** 

**Student ID: 801238561** 

## Approach and Implementation
I started out trying to learn how to use Hadoop, and ended up making my mappers and reducers easy to understand for myself. Essentially, this MapReduce assignment focuses mainly on three jobs, each with a mapper and a reducer, and the last one with just a mapper.
### Mapper Design
- I had 3 Mapper's in my final solution
    1. UniqueTermsMapper: (word,document#), process a line and get all unique words belonging to the document and ignore repetitions by use of hashset
    2. DocSizePairMapper: (DOCSIZE or DOCPAIR,document Name or docA,docB)
    3. ComputeJacardyMapper:(A + ", " + B,"Similarity": jacardy). Here we simply compute the Jaccard similarity by using probabilistic equation to do (||A| + |B| - |intersection|)/|intersection|

### Reducer Design
- I had 2 Reducers in my final solution.
    1. UniqueTermsReducer: (DOCSIZE,docuemnt#) and (DOCPAIR,document pair), The first is used in the process of trying to calculate the |A| essentially and the second is used for listing all pairs of documents
    2. DocSizePairReducer: (SIZEOFDOC, document# + \t+ count) or (INTERSECTION, A and B count) here, we attempt to get the numerical value forthe  unique size of the document and also the count of the INTERSECTION pairs, which when you count them up you get |A and B|

### Overall Data Flow
[Describe how data flows from the initial input files, through the Mapper, shuffle/sort phase, and the Reducer to produce the final output.]
Data Flot:
INPUT FILES -> UniqueTermsMapper ->  UniqueTermsReducer -> DocSizePairMapper -> DocSizePairReducer -> setup() -> ComputeJacardyMapper() -> j3.setNumReduceTasks(0);
---

## Setup and Execution

### ` Note: The below commands are the ones used for the Hands-on. You need to edit these commands appropriately towards your Assignment to avoid errors. `

### 1. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Build the Code and Copy JAR to Docker Container using bash script**

Build the code using Maven:

```bash
sh script1.sh
```

### 3. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
docker cp ./dataset resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 4. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 5. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hdfs dfs -mkdir -p /input/dataset
```

Copy the input dataset to the HDFS folder:

```bash
hdfs dfs -put -f ./dataset/dataset_1_1000_words.txt /input/dataset/
hdfs dfs -put -f ./dataset/dataset_2_3000_words.txt /input/dataset/
hdfs dfs -put -f ./dataset/dataset_3_5000_words.txt /input/dataset/
```

### 6. **Execute the MapReduce Job**

Run your MapReduce job using the following commands:

```bash
chmod 777 script2.sh
./script2.sh
```


### 7. **To view output, Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output /opt/hadoop-3.2.1/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output/ ./datasets/
    ```
3. Commit and push to your repo so that we can able to see your output


---

## Challenges and Solutions

[Describe any challenges you faced during this assignment. This could be related to the algorithm design (e.g., how to generate pairs), implementation details (e.g., data structures, debugging in Hadoop), or environmental issues. Explain how you overcame these challenges.]
1. Had issues with bulding the jar
    - Solved it by changing the location of all my java files into the path maven specifies
2. Codespaces kept crashing every 20 mins
3. Had a hard time with path reading from my mapper or my driver.
    - had to debug using llm, changed from FileReader to Hadoop's FileSystem.
---
## Sample Input

**Input from `small_dataset.txt`**
```
Document1 This is a sample document containing words
Document2 Another document that also has words
Document3 Sample text with different words
```
## Sample Output

**Output from `small_dataset.txt`**
```
"Document1, Document2 Similarity: 0.56"
"Document1, Document3 Similarity: 0.42"
"Document2, Document3 Similarity: 0.50"
```
## Obtained Output: (Place your obtained output here.)

1. Output Folder contains the data from the Map Reduce specifically in the file labeled: part-m-00000

#### Report your observations on the diLerence in execution times between 3 data nodes and 1 data node in the README file.

1. 3 Data nodes(wall_seconds column == time taken)

| dataset                  | wall_seconds | stage2_dir                                   | final_dir                                             | exit_code |
|---------------------------|--------------|----------------------------------------------|-------------------------------------------------------|-----------|
| dataset_1_1000_words.txt | 45           | /output/ds_stage2_dataset_1_1000_words_txt   | /output/ds_similarity_dataset_1_1000_words_txt        | 0         |
| dataset_2_3000_words.txt | 44           | /output/ds_stage2_dataset_2_3000_words_txt   | /output/ds_similarity_dataset_2_3000_words_txt        | 0         |
| dataset_3_5000_words.txt | 46           | /output/ds_stage2_dataset_3_5000_words_txt   | /output/ds_similarity_dataset_3_5000_words_txt        | 0         |

2. 1 Data Node(wall_seconds column == time taken)

| dataset                  | wall_seconds | stage2_dir                                   | final_dir                                             | exit_code |
|---------------------------|--------------|----------------------------------------------|-------------------------------------------------------|-----------|
| dataset_1_1000_words.txt | 67           | /output/ds_stage2_dataset_1_1000_words_txt   | /output/ds_similarity_dataset_1_1000_words_txt        | 0         |
| dataset_2_3000_words.txt | 62           | /output/ds_stage2_dataset_2_3000_words_txt   | /output/ds_similarity_dataset_2_3000_words_txt        | 0         |
| dataset_3_5000_words.txt | 66           | /output/ds_stage2_dataset_3_5000_words_txt   | /output/ds_similarity_dataset_3_5000_words_txt        | 0         |

3. It seems that with the reduction in data nodes from 3 to 1, the time taken increased by a factor of 1/3.
