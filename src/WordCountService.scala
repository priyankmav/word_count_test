import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._



object WordCountService {
  def countWords(url: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
      import sqlContext.implicits._
      val linesDF = sc.textFile(url).toDF("line")
      val wordsDF = linesDF.explode("line","word")((line: String) => line.split(" "))
      val wordCountDF = wordsDF.groupBy("word").count()
      wordCountDF
      }
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
 //set spark configuration
    // make spark context
    val sparkContext = new SparkContext(sparkConf)
    // make context
    val sqlContext = new SQLContext(sparkContext)
    var url = "/Users/priyank.mavani/Desktop/Anthem/slyfox.txt"
    val word_count = countWords(url, sparkContext, sqlContext)
    word_count.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true").save("/Users/priyank.mavani/Desktop/Anthem/counted_sly_fox")
    

    
}
}