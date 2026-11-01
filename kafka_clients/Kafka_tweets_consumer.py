from kafka import KafkaConsumer
from json import loads
from rich import print

consumer = KafkaConsumer(
    'my-topic-test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='twitter-consumer-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print("[green]✅ Listening to fake Twitter stream[/green]\n")

try:
    for message in consumer:
        t = message.value
        print(f"[bold]{t.get('created_at','')}[/bold]  @{t.get('username','')}")
        print(f"  {t.get('tweet','')}")
        print(f"  lang={t.get('lang','')}  location={t.get('location','')}")
        print(f"  hashtags={t.get('hashtags',[])}")
        print("-" * 60)

except KeyboardInterrupt:
    print("\n[yellow]🛑 Consumer arrêté[/yellow]")
finally:
    consumer.close()
