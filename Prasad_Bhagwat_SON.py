# Imports required for the program
from pyspark import SparkConf, SparkContext
from operator import add
from datetime import datetime
import itertools
import sys
import os

# Macro definitions
n_C_2 = [0, 0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136, 153, 171, 190, 210]

# Concatenate the frequent items in the expected output format
def concat_parenthesis(freq_item):
    return str('(' + str(freq_item) + ')')


# String conversion of frequent items from int
def convert_int_to_str(item):
    return str(item)


# Couting candidates in the generated RDD
def count_candidates(ratings_rdd, combinations):
    baskets, result = list(ratings_rdd), []
    # Looping over all combinations
    for combonation in combinations:
        occurance = 0
        # Looping over all baskets
        for basket in baskets:
            temp_itemset = set(combonation)
            for item in combonation:
                if item in basket: temp_itemset.remove(item)
            # Updating occurance value
            if not temp_itemset: occurance += 1
        result.append((combonation, occurance))
    yield result


# Generating candidate tuples for counting
def gen_candidates(freq_items, tuple_size):
    count, candidate_list, freq_item_list   = {}, [], freq_items.collect()
    # Looping over frequent item list to generate all the combinations of item_set
    for part_1, part_2 in itertools.product(freq_item_list, freq_item_list):
        # Creating item_set of higher order by keeping part_1 common and changing part_2
        item_set        = tuple(sorted(set(part_1 + part_2)))
        count[item_set] = count[item_set] + 1 if item_set in count else 1
    
    # Checking whether generated item_set is frequent and then appending in candidate list for actual counting
    for counter_size in count:
        # Checking tuple size and n_C_2 array before appending into candidate list
        if tuple_size is len(counter_size) and count[counter_size] >= n_C_2[tuple_size]:
            candidate_list.append(counter_size)

    return candidate_list


# Main function
def main():
    start_time          = datetime.now()

    # Command Line Arguments
    case_number         = int(sys.argv[1])
    input_file          = sys.argv[2]
    support_threshold   = int(sys.argv[3])

    # Output filename generation
    output_file_name    = "Prasad_Bhagwat_SON_" + os.path.splitext(os.path.basename(input_file))[0] + ".case" + str(case_number) + "-" + str(support_threshold) + ".txt"
    output_file         = open(output_file_name, 'w')
    output_result       = ""

    # Creating Spark Context
    spark_config  = SparkConf()
    spark_context = SparkContext(conf = spark_config)
    input_data    = spark_context.textFile(input_file).coalesce(8)

    # Extracting header
    data = input_data.filter(lambda x: "userId" not in x)

    # Generating RDD based on input "Case" either (UserID, MovieID) or (MovieID, UserID)
    if case_number == 1:
        ratings_rdd     = data.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).groupByKey().map(lambda x: list(set(x[1]))).persist()
    else:
        ratings_rdd     = data.map(lambda x: (int(x.split(",")[1]), int(x.split(",")[0]))).groupByKey().map(lambda x: list(set(x[1]))).persist()

    # Generating frequent items of order 1
    ratings_list_rdd    = ratings_rdd.flatMap(list).groupBy(lambda x: x).mapValues(len).filter(lambda x: x[1] >= support_threshold).keys().persist()
    ratings_list        = sorted(ratings_list_rdd.collect())

    # Generating expected format output string for all frequent itemsets of order 1
    result 	        = map(concat_parenthesis, ratings_list)
    output_result   += str(', '.join(result)) + "\n"+ "\n"

    # Generating combinations of frequent items of size 1 for getting pairs using Combinations() function
    combinations    = list(itertools.combinations(ratings_list, 2))
    freq_items 	    = ratings_rdd.mapPartitions(lambda x : count_candidates(x, combinations)).flatMap(lambda x: x).reduceByKey(add).filter(lambda x: x[1] >= support_threshold).map(lambda x: x[0])

    tuple_size     = 2
    next_iteration  = True

    # Generating frequent itemsets of higher order
    while next_iteration:
        candidate       = gen_candidates(freq_items, tuple_size)
        freq_items      = ratings_rdd.mapPartitions(lambda x : count_candidates(x, candidate)).flatMap(lambda x: x).reduceByKey(add).filter(lambda x: x[1] >= support_threshold).map(lambda x: x[0])
        # Sorting output list of frequent items
        result_list     = sorted(freq_items.collect())
        # Check whether frequent items list is empty for current order of itemsets
        if not result_list:
            next_iteration = False
        else:
            tuple_size += 1

        # Generating output in the expected form i.e. "(item1, item2)" or "(item1, item2, item3)" etc.
        frequency       = map(convert_int_to_str, result_list)
        output_result   += str(', '.join(frequency)) + "\n"+ "\n"

    # Writing result to output file
    output_result       = output_result[:-4]
    output_file.write(output_result)
    output_file.close()
    # Printing time taken by program
    print "---------------Start Time--------------", start_time
    print "---------------End Time--------------", datetime.now()


# Entry point of the program
if __name__ == '__main__':
    main()
