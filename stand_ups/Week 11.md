Last week, I worked on creating more Airflow tasks as planned. However, when I tried running the data transformation part again, I encountered some very tricky errors related to incompatible or conflicting connector JAR files. After trying different JAR versions, I managed to resolve the issue with the Kafka-Spark connector, but it took me three days.

Now, I'm debugging the Cassandra-Spark connector errors, which are even more complicated. The logs are unclear, and there isn’t much help available on the interne.

This week and next week, I'll continue debugging. I've also found a similar project from someone else, and I’m running it on my local machine to identify the appropriate JAR files to use.