[![Space Filling Curve CI](https://github.com/dwsmith1983/space-filling-curves/actions/workflows/scala-test.yml/badge.svg?branch=main&event=pull_request)](https://github.com/dwsmith1983/space-filling-curves/actions/workflows/scala-test.yml)
# Space Filling Curves
Space filling curves allow us to represent an n-dimensional curve
in one dimensional while preserving locality. Techniques such as
`z-ordering` allow big data platforms to efficiently store and 
process large chunks of data.
1. [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)
2. [Z-order curve](https://en.wikipedia.org/wiki/Z-order_curve)
3. [Z-order indexing for multifaceted queries in Amazon DynamoDB: Part 1](https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-1/)
4. [Z-order indexing for multifaceted queries in Amazon DynamoDB: Part 2](https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-2/)

# Work in Progress
* Currently, built against Spark 3.1.0 and Scala 2.12.13
* Need to create crosscompiled version on Spark 2.x and Scala 2.11/2.12
* Finish README
* Finish tests