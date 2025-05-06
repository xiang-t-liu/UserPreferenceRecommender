import Utils.writeToParquet
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

case class ProductProperty(sku: Long, name: String = "")
case class PageVisit(url: Long, client_id: String)

object UserPreferences {
  val resPath = "$YOUR_FINAL_RESULT_PATH"
  val pageVisitFilePath = "$PAGE_VISIT_PARQUET_FILE_PATH"
  val productPropertyFilePath = "$PRODUCT_PROPERTY_FILE_PATH"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Examples")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val productProperties = spark
      .read.parquet(productPropertyFilePath)
      .select("sku", "name")
      .as[ProductProperty]

    val pageVisits = spark
      .read.parquet(pageVisitFilePath)
      .select("url", "client_id")
      .as[PageVisit]

    val res = productProperties.join(
      pageVisits,
      joinExprs = col(colName = "sku") === col(colName = "url"),
      joinType = "inner"
    )

    writeToParquet(res, resPath)

    spark.stop
    System.exit(0)
  }
}
