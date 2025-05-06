## Build ##

This is an SBT project.
```
sbt clean compile
```
```
sbt run
```

## Usage ##

"UserPreferences.scala" provided in this package contains sample code for reading parquet files and join two dataframes. 

```scala
//read data file in as a dataframe, and parse data and create ProductProperty case class
val productProperties = spark
      .read.parquet(productPropertyFilePath)
      .select("sku", "name")
      .as[ProductProperty]
```

```scala
//read data file in as a dataframe, and parse data and create ProductProperty case class
val productProperties = spark
      .read.parquet(productPropertyFilePath)
      .select("sku", "name")
      .as[ProductProperty]
```scala
// join two dataframes
val res = productProperties.join(
      pageVisits,
      joinExprs = col(colName = "sku") === col(colName = "url"),
      joinType = "inner"
    )
```


After the join, the dataset looks like this:

```
+---+--------------------+---+---------+
|sku|                name|url|client_id|
+---+--------------------+---+---------+
| 22|[ 88 113  45 178 ...| 22| 13680214|
| 22|[ 88 113  45 178 ...| 22| 13680214|
| 34|[  1  45 212 178 ...| 34| 16133051|
| 54|[212 149  92  51 ...| 54| 10065480|
| 77|[233 233 144 233 ...| 77| 15755767|
| 77|[233 233 144 233 ...| 77| 15755767|
| 77|[233 233 144 233 ...| 77| 15755767|
|113|[236 210 191 192 ...|113| 15829681|
|113|[236 210 191 192 ...|113|  2111097|
|155|[ 85 117 141 185 ...|155|   851608|
|167|[ 47 198 228 211 ...|167| 10064043|
|167|[ 47 198 228 211 ...|167| 11085208|
|167|[ 47 198 228 211 ...|167| 10064043|
|167|[ 47 198 228 211 ...|167| 11085208|
|167|[ 47 198 228 211 ...|167| 10064043|
|191|[110 191  99 160 ...|191|  9265052|
|191|[110 191  99 160 ...|191|  5683991|
|191|[110 191  99 160 ...|191|  9265052|
|191|[110 191  99 160 ...|191| 19263604|
|191|[110 191  99 160 ...|191|  9265052|
+---+--------------------+---+---------+
```

