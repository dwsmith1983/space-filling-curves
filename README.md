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

# Usage
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

# Work in Progress
* README
* Better organization
* Add other space filling curves: Hilbert, GeoHash, Peano

# Help Needed
Looking for help with those experienced with creating decent READMEs
and publishing code to Maven.
