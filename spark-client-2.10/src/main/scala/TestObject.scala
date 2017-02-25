import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object TestObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestSparkApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val r = sc.textFile("src/test/resources/TestFile.txt")
    val rcount = r.flatMap { x => x.split(" ") }
                  .map ( word => (word,1) )
                  .reduceByKey(_+_)
                                        
    print(rcount.collect().mkString(","))        
  }
}