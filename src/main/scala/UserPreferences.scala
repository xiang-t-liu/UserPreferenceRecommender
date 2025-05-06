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
//    sample result of res:
//    +-----+--------------------+-----+---------+
//    |  sku|                name|  url|client_id|
//    +-----+--------------------+-----+---------+
//    | 3069|[194 185 185  64 ...| 3069| 13304773|
//    | 3069|[194 185 185  64 ...| 3069| 23382978|
//    | 3091|[153 177   9 153 ...| 3091| 19988807|
//    | 4823|[ 88 252  42 214 ...| 4823| 23630887|
//    | 6183|[ 46 152 155  68 ...| 6183|  7596270|
//    | 8777|[  9  14 151 192 ...| 8777| 10663649|
//    | 8907|[ 70  70  70  70 ...| 8907|  1202249|
//    | 9526|[254 254 254 254 ...| 9526| 17982446|
//    |12260|[252  91 108 252 ...|12260|  6082116|
//    |12260|[252  91 108 252 ...|12260|  9903627|
//    |13845|[203 167 224 245 ...|13845|  6840978|
//    |16490|[157  91  91  91 ...|16490| 11671997|
//    |26351|[128 157  24  89 ...|26351| 17482489|
//    |31456|[181 181  74 127 ...|31456| 15660000|
//    |34312|[ 53 234  42 236 ...|34312| 16826762|
//    |35250|[216  16 197  12 ...|35250| 20768709|
//    |35726|[ 97 105 135 136 ...|35726|  8318885|
//    |36278|[ 16  65  24 208 ...|36278|   878327|
//    |37521|[ 13 171 243 149 ...|37521|  5330842|
//    |37668|[185 141  57   7 ...|37668|  6040181|
//    +-----+--------------------+-----+---------+

    writeToParquet(res, resPath)

    spark.stop
    System.exit(0)
  }
}
