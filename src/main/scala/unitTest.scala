import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
//import org.apache.spark

//import scala.reflect.io.Path
object unitTest{
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local[*]")
      .appName("testcase")
      .getOrCreate()
//    val sc=spark.sparkContext
    val path=getClass.getResource("/FINAL.csv")
    val ppath=path.getPath
    val gpath=getClass.getResource("/abbot.csv")
    val gfp=gpath.getPath
//    println(sc.getClass.getName)
    val df=CsvReader(ppath,gfp,spark)
    df.show()
  }
  def CsvReader(path: String,gfp:String, ss: SparkSession): DataFrame= {
    val rf = ss.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load(path)

    val gf=ss.read.format("image").option("dropInvalid", true).load(gfp)
    //    println(rf.getClass.getName)
    println(rf.columns.size)
    if (rf.columns.size > 20) {
      return rf
    }
    else {
      println("insufficient columns")
      return gf
    }

  }


  }


