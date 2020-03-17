import org.scalatest.FunSuite
import org.scalatest.FeatureSpec
import unitTest.getClass
import org.apache.spark.sql.SparkSession
//trait SparkSessionTestWrapper{
//  lazy val spark: SparkSession={
//    SparkSession
//      .builder()
//      .master("local")
//      .appName("spark df test")
//      .getOrCreate()
//  }
//}
class DfTest extends FunSuite  {
  val spark1= SparkSession
    .builder()
    .master("local")
    .appName("test")
    .getOrCreate()
  val path=getClass.getResource("/FINAL.csv")
  val ppath=path.getPath
  val gpath=getClass.getResource("/abbot.csv")
  val gfp=gpath.getPath
  test("dataframe.column.size"){
    assert(unitTest.CsvReader(ppath,gfp,spark1).columns.size===21)
  }

}
