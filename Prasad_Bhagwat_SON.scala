// Imports required for the program
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark.rdd.RDD
import scala.collection.mutable


object FrequentItemsetsSON {

  // Macro definitions
  val n_C_2 = Array(0, 0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136, 153, 171, 190, 210)

  // Sorting the elements of result
  def sort[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted

  // Couting candidates in the generated RDD
  def count_candidates(ratings_rdd: Iterator[Set[Int]], combinations: List[Set[Int]]): Iterator[(Set[Int], Int)] = {
    var result  : mutable.HashMap[Set[Int], Int] = new mutable.HashMap[Set[Int], Int]()
    // Looping over all combinations
    for (basket <- ratings_rdd) {
      // Looping over all baskets
      for (combination <- combinations) {
	// Updating occurance value
        if (combination.subsetOf(basket)) {
          result(combination) = result.getOrElse(combination, 0) + 1
        }
      }
    }
    result.iterator
  }

  // Generating candidate tuples for counting
  def gen_candidates(freq_items: RDD[Set[Int]], touple_size: Int): Iterator[Set[Int]] = {
    val count           = scala.collection.mutable.HashMap[Set[Int], Int]()
    var candidate_list  : List[Set[Int]] = List()
    val freq_item_list  = freq_items.collect()
    var item_set        = Set[Int]()

    // Looping over frequent item list to generate all the combinations of item_set
    for ( part_1 <- freq_item_list; part_2 <- freq_item_list) {
      // Creating item_set of higher order by keeping part_1 common and changing part_2
      item_set          = part_1.union(part_2)
      var counter : Int = if ( count.contains(item_set)) count(item_set)
      else 1
      count.put(item_set, counter + 1)
    }

    // Checking whether generated item_set is frequent and then appending in candidate list for actual counting
    for ( counter_size <- count) {
      // Checking tuple size and n_C_2 array before appending into candidate list
      if ( touple_size == counter_size._1.size && counter_size._2 >= n_C_2(touple_size)) {
        candidate_list = candidate_list:+(counter_size._1)
      }
    }
    candidate_list.iterator
  }

  // Main function
  def main(args: Array[String]){
    // Command Line Arguments
    val case_number         = args(0).toInt
    val input_file          = args(1)
    val support_threshold   = args(2).toInt

    val index : Int         = input_file.lastIndexOf("/")
    val input_file_name     = input_file.substring(index + 1)

    // Output filename generation
    val output_file_name    = "Prasad_Bhagwat_SON_" + input_file_name.replaceAll("(\\.[^\\.]*$)", "") + ".case" + case_number.toString + "-" + support_threshold.toString + ".txt"
    val output_file         = new PrintWriter(new File(output_file_name))
    var output_result       = ""

    // Creating Spark Context
    val spark_config        = new SparkConf()
    val spark_context       = new SparkContext(spark_config)
    val input_data          = spark_context.textFile(input_file).coalesce(8)

    // Extracting header
    var data                = input_data.filter(x => ! x.contains("userId"))

    // Generating RDD based on input "Case" either (UserID, MovieID) or (MovieID, UserID)
    val ratings_rdd = data.map(x=>{
      if ( args(0) == "1") {
        val y = x.split(",")
        (y(0).toInt,y(1).toInt)}
      else if ( args(0) == "2") {
        val y = x.split(",")
        (y(1).toInt,y(0).toInt)
      }
      else
        (0,0)
    }).groupByKey().map(x => x._2.toSet).persist()

    // Generating frequent items of order 1
    var ratings_list_rdd = ratings_rdd.flatMap(_.toList).groupBy(identity).mapValues{_.size}.filter(x => x._2 >= support_threshold).keys.persist()
    var ratings_list     = ratings_list_rdd.collect()

    // Generating expected format output string for all frequent itemsets of order 1
    var result           = ratings_list.map(x => Set(x))
    var temp_result      = sort(result).map(x => x.toArray.mkString("(", ", ", ")")).mkString(", ")
    output_result        = output_result.concat(temp_result).concat("\n\n")

    // Generating combinations of frequent items of size 1 for getting pairs using Combinations() function
    val combinations     = ratings_list.combinations(2).map(_.toSet).toList
    var freq_items       = ratings_rdd.mapPartitions(x => count_candidates(x, combinations)).flatMap(x => Set(x))
      .reduceByKey(_+_).filter(x => x._2 >= support_threshold).map(x => x._1)

    var touple_size      = 2
    var next_iteration   = true

    // Generating frequent itemsets of higher order
    while (next_iteration) {
      val candidate           = gen_candidates(freq_items, touple_size).toList
      // Sorting output list of frequent items
      val final_freq_items    = ratings_rdd.mapPartitions(x => count_candidates(x, candidate)).flatMap(x => Set(x))
        .reduceByKey(_+_).filter(x => x._2 >= support_threshold).map(x => x._1.toSeq.sorted)
      val result_list         = final_freq_items.collect

      // Check whether frequent items list is empty for current order of itemsets
      if (result_list.isEmpty) {
        next_iteration = false
      }
      else {
        touple_size    += 1
      }

      // Generating output in the expected form i.e. "(item1, item2)" or "(item1, item2, item3)" etc.
      val sorted_list = sort(result_list.toList)
      val temp_result = sorted_list.map(x => x.toArray.mkString("(", ", ", ")")).mkString(", ")//.map(x => x.toString)
      output_result   = output_result.concat(temp_result).concat("\n\n")
    }

    // Writing result to output file
    output_result     = output_result.stripLineEnd
    output_result     = output_result.stripLineEnd
    output_result     = output_result.stripLineEnd
    output_result     = output_result.stripLineEnd
    output_file.write(output_result)
    output_file.close()
  }
}
