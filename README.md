[![Space Filling Curve CI](https://github.com/dwsmith1983/space-filling-curves/actions/workflows/scala-test.yml/badge.svg?branch=main)](https://github.com/dwsmith1983/space-filling-curves/actions/workflows/scala-test.yml)
# Space Filling Curves
Space filling curves allow us to represent an n-dimensional curve
in one dimensional while preserving locality. Techniques such as
`z-ordering` allow big data platforms to efficiently store and 
process large chunks of data.
1. [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)
2. [Z-order curve](https://en.wikipedia.org/wiki/Z-order_curve)
3. [Z-order indexing for multifaceted queries in Amazon DynamoDB: Part 1](https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-1/)
4. [Z-order indexing for multifaceted queries in Amazon DynamoDB: Part 2](https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-2/)

# Available GitHub Packages
```
Spark-2.3.1 on Scala 2.11.12 
Spark-2.4.7 on Scala 2.11.12 and Scala 2.12.13
Spark-3.1.0 on Scala 2.12.13 Java 11 version 0.1.0 and 0.2.0
```

# Usage
How to determine Morton (Z) or Hilbert Ordering.
## Morton (Z Order)
Given the dataframe below, we want to Morton (Z Order) our data by `id`, `x`, `y`
```scala
// Currently, this isn't setup to use Maven. 
// For now, publish local or just assembly and use the jar.
val orderingCols: Array[String] = Array("id", "x", "y")
val df: DataFrame = Seq(
  (1, 1, 12.23, "a", "m"),
  (4, 9, 5.05, "b", "m"),
  (3, 0, 1.23, "c", "f"),
  (2, 2, 100.4, "d", "f"),
  (1, 25, 3.25, "a", "m")
).toDF("x", "y", "amnt", "id", "sex")

val mortonOrdering: Morton = new Morton(df, orderingCols)
// this will order your whole dataframe by the z_index
val zIndexedDF: DataFrame = mortonOrdering
  .mortonIndex.sort("z_index")
```
## Hilbert Order
Hilbert is only available in version 0.2.0 on Spark 3.

# Benefits
How do space filling curves benefit? Let's consider the Chicago crime data set available
at [Crimes - 2001 to Preset](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2).
This data was pulled on 8 August 2021. The downloaded `csv` file is `1.74` GB and `7374374`
records. First, I converted the `csv` to `parquet` with defualt compression of `snappy`.

| File Type  | Compression | Number of Leaf Files | Optimization | Size (MB) |
| ---------- | ----------: | -------------------: | -----------: | --------: |
| CSV        | None        | 1                    | None         | 1781.76   |
| Parquet    | Snappy      | 13                   | None         | 470.02    |
| Parquet    | gzip        | 13                   | None         | 315.22    |
| Parquet    | gzip        | 1                    | Semi-linear  | 269.81    |
| Parquet    | gzip        | 1                    | Z-order      |           |
| Parquet    | gzip        | 1                    | Hilbert      |           |

which resulted in `13` leaf files all approximately `38` MB for a total size of `0.459` GB.

# Work in Progress
* README
* Better organization

# Help Needed
Looking for help with those experienced with creating decent READMEs
and publishing code to Maven.
