from kafka import KafkaConsumer
from json import loads, dumps
from rich import print
import pydoop.hdfs as hdfs

consumer = KafkaConsumer(
    'my-topic-test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

HDFS_DIR = 'hdfs://localhost:9000/kafka_demo'
HDFS_FILE = HDFS_DIR + '/tweets_data.jsonl'

print("[green]✅ Kafka consumer -> HDFS started[/green]\n")

# créer dossier si besoin
if not hdfs.path.exists(HDFS_DIR):
    hdfs.mkdir(HDFS_DIR)

for message in consumer:
    tweet = message.value
    print(tweet)

    line = dumps(tweet, ensure_ascii=False) + "\n"

    # ✅ créer si n'existe pas, sinon append
    mode = 'at' if hdfs.path.exists(HDFS_FILE) else 'wt'

    with hdfs.open(HDFS_FILE, mode) as f:
        print("[cyan]Storing in HDFS![/cyan]")
        f.write(line)
