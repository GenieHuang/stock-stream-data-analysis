Last week, I tried different ways to connect Cassandra with Spark. But even though I found a similar project using both Cassandra and Spark, I couldn’t get it to run either.

So I decided to change the pipeline structure, and changed to load the data into Amazon S3 instead. Last week I focused on rebuilding the pipeline and connecting Spark with S3.

The issue I encountered was also the incompatible JAR versions of the kafka and S3 connector. But S3 is more widely used than Cassandra, there were more resources and posts on the internet to help me debug. Now I’ve successfully connected Spark to S3.

The plan for next week is to keep working on S3 and isolating every part using Docker. Hopefully by next week I’ll have the data pipeline fully running.