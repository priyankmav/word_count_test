import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.junit.Test
import junit.framework.TestCase
import org.junit.Assert._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

object WordCountServiceTest {
  
  @Test
    def countWordsTest(sparkContext: SparkContext, sqlContext: SQLContext) {

    //input to test the function
    val url = "/Users/priyank.mavani/Desktop/Anthem/slyfox.txt"
    
    //running the function countWords from the WordCountService object in order to test the function
    val counts = WordCountService.countWords(url, sparkContext, sqlContext)
    
    //casting the column to be tested as integer
    val counts_main = counts.withColumn("countTmp", counts.col("count").cast(IntegerType)).drop("count").withColumnRenamed("countTmp", "count")
    
    //Going to test two counts for two words, "the" and "and".
    //Extracting counts from the test and assigning it to value_1, value_2 
    val value_1 = counts_main.filter(col("word") === "the").select("count").collect()(0)(0)
    val value_2 = counts_main.filter(col("word") === "and").select("count").collect()(0)(0)
    
    //loading actual output from the WordCountService object and assigning it as input in order to test.
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("infer Schema", "true").load("/Users/priyank.mavani/Desktop/Anthem/counted_sly_fox/")
    //casting the column count to integer
    val df_main = df.withColumn("countTmp", df.col("count").cast(IntegerType)).drop("count").withColumnRenamed("countTmp", "count")
    
    //loading the counts for two words and assigning it to variables.
    val word_one_test = df_main.filter(col("word") === "the")
    val word_two_test = df_main.filter(col("word") === "and")
    val word_one_test_assert = word_one_test.select("count").collect()(0)(0)
    val word_two_test_asset = word_two_test.select("count").collect()(0)(0)
    
    //finally testing the values and checking if they are equal or not (value from the function tested versus value loaded from the actual output)
    assertEquals(value_1, word_one_test_assert)
    assertEquals(value_2, word_two_test_asset)
    
  }
    
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    //set spark configuration
    // make spark context
    val sparkContext = new SparkContext(sparkConf)
    // make context
    val sqlContext = new SQLContext(sparkContext)
    val countWordsTestService =  countWordsTest(sparkContext, sqlContext)
    

}

    
}