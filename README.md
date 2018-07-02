## Spark-Streaming-Kafka

This example is focussed around a generic design pattern to implement Spark Streaming applications using Kafka Queue. The approach is inspired from need to multiple data pipeline on the streaming data.

E.g., Data pipeline could include.

1. Data Quality
2. Data Transformations
3. Reporting Stats
4. Persistence 

And more...

This example assumes JSON as the input data format, which is the most common scenario, however, this example can be conveniently modified with customer parsers.

Also, for those who are seeking around Exactly Once semantics, this implementation will be very useful to achieve that. Keeping this focus in mind, the offset management was done using Zookeeper. Well, there plethora of ways this could be managed, but we felt Zookeeper would be the optimal way to do it. The other options of externally managing the offsets were ruled out especially when it comes to storing the offsets in RDBMS & NoSQL. However, it would be expensive to maintain a separate schema for this purpose, but you may have better control over your offsets with some trade-off.

A brief note below on the options explored before building a rationale on Zookeeper,

#### Checkpointing

This is a very good option for streams are not read from message queues, e.g., Socket Streaming, Sensor Signals or any source that do not persist the events. There is a high possibility of losing the data.
Checkpoint can be enabled with WAL(Write Ahead Logs) to ensure the recovery of messages during restart.

The drawbacks of checkpoints to keep in mind are,

- Redundancy
- Format - Checkpoint stores the data in Serialized Byte format 

#### Asynch Commits to Kafka

Due to the asynchronous nature of the commits, an extensive load testing revealed some duplicates when rebalancing was triggered due to various reasons. So wouldn't recommend for Exactly Once semantics especially on large clusters used by many users. However, this option could be a choice for Atleast Once Semantics.
A trivial note, Kafka doesn't allow storing metadata with offsets which is possible through offset management in external repositories like Zookeeper, HDFS, RDBMS, NoSQL etc.