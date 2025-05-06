import org.apache.spark.sql.DataFrame

object Utils {
  def writeToParquet(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write.parquet(s"$path.parquet")
  }
}
