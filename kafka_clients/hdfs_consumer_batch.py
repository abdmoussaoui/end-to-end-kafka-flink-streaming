from kafka import KafkaConsumer
from json import loads, dumps
from rich import print
import pydoop.hdfs as hdfs
import time

consumer = KafkaConsumer(
    'my-topic-test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='twitter-hdfs-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

NAMENODE = "localhost"  # change si besoin
HDFS_DIR = f"hdfs://{NAMENODE}:9000/kafka_demo"
BATCH_SIZE = 50

hdfs.mkdir(HDFS_DIR)
print(f"[green]✅ Kafka -> HDFS batch consumer started[/green] dir={HDFS_DIR}\n")

buffer = []
file_index = int(time.time())

try:
    for msg in consumer:
        tweet = msg.value
        buffer.append(tweet)
        print(f"📥 @{tweet.get('username')}")

        if len(buffer) >= BATCH_SIZE:
            hdfs_file = f"{HDFS_DIR}/tweets_{file_index}.jsonl"

            with hdfs.open(hdfs_file, "wt") as f:
                for t in buffer:
                    f.write(dumps(t) + "\n")

            print(f"[cyan]📦 Batch stored in HDFS: {hdfs_file}[/cyan]\n")
            buffer.clear()
            file_index += 1

except KeyboardInterrupt:
    print("\n[yellow]🛑 Stopped[/yellow]")
finally:
    consumer.close()
