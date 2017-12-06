kafka-topics --zookeeper localhost:2181 \
             --create \
             --topic article-change-counts \
             --replication-factor 1 \
             --partitions 1 \
             --config cleanup.policy=compact 
