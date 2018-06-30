SON Algorithm for generating Frequent Items
==========================================================

### Enviroment versions required:

Spark: 2.2.1  
Python: 2.7  
Scala: 2.11

### Algorithm implementation approach:

The implementation approach I used is same for both Python and Scala is as follows:  
For implementing SON algorithm, first I implemented A-Priori algorithm and tested it on Small1.csv for the compliance. Then I created RDD from given input file based on the input case number. Using mapPartition function of Spark for dividing input data into multiple chunks and then applying A-Priori algorithm on each chunk I got all sizes of frequent itemsets from the given dataset. For implementing this process, I generated the singleton frequent items from the dataset first and then generated candidate itemsets of size two using combinations of all singleton frequent items. Then I divided the data into chunks using mapPartition function for counting the actual number of occurances of each of these candidate itemsets of size two in the input dataset along with a Combine function to give out count for each of the candidate itemset in the chunk of data and used Reduce function of Spark to add up the count of each of the individual candidate itemsets giving out the total number of occurances for each of these candidate itemsets. Then, I compared this count with the Support Threshold to filter out the False Positive itemsets from this list to give out the actual Frequent itemsets of size two. I repeated the same process for generating the Frequent itemsets of size three and so above till there are no more one big size Frequent itemset than the current size. Finally I combined all the frequent itemsets and wrote them in the output text file in the sorted order. For optimizing the above process I used various collections like Sets, Lists and Dictionaries for python and immutable Sets, Lists and Hashmaps for scala. As scala uses immutable collections the results are generated a lot faster as compared to python.

### Dataset used for testing:
[MovieLens](https://grouplens.org/datasets/movielens/) 


### Python command for executing SON Algorithm

* * *

Exceuting SON algorithm using _“Prasad\_Bhagwat\_SON.py”_ file

    spark-submit Prasad_Bhagwat_SON.py <case number> <csv file path> <support threshold>
    

where,  
_1. 'case number'_ corresponds to which _‘case’_ to execute for generating frequent items  
for generating combinations of frequent movies choose _'1'_   
for generating combinations of frequent users choose _'2'_   
_2. 'csv file path'_ corresponds to the absolute path of input _‘ratings.csv’_ file  
_3. 'support threshold'_ corresponds to the support required item to be considered as Frequent Item

Example usage of the above command is as follows:

     ~/Desktop/spark-2.2.1/bin/spark-submit Prasad_Bhagwat_SON.py 2 /home/prasad/workspace/DataMining/Assignment2/MovieLens.Small.csv 200
    

Note : _Output file_ named _‘Prasad\_Bhagwat\_SON_MovieLens.Small.case2-200.txt’_ is generated at the location from where the _spark-submit_ is run.

### Scala command for executing SON Algorithm

* * *

Exceuting SON algorithm using _“Prasad\_Bhagwat\_SON.jar”_

    spark-submit --class FrequentItemsetsSON Prasad_Bhagwat_SON.jar <case number> <csv file path> <support threshold>
    

where,  
_1. 'case number'_ corresponds to which _‘case’_ to execute for generating frequent items  
for generating combinations of frequent movies choose _'1'_   
for generating combinations of frequent users choose _'2'_   
_2. 'csv file path'_ corresponds to the absolute path of input _‘ratings.csv’_ file  
_3. 'support threshold'_ corresponds to the support required item to be considered as Frequent Item

Example usage of the above command is as follows:

     ~/Desktop/spark-2.2.1/bin/spark-submit --class FrequentItemsetsSON Prasad_Bhagwat_SON.jar 1 /home/prasad/workspace/DataMining/Assignment2/MovieLens.Big.csv 30000
    

Note : _Output file_ named _‘Prasad\_Bhagwat\_SON_MovieLens.Big.case1-30000.txt’_ is generated at the location from where the _spark-submit_ is run.
