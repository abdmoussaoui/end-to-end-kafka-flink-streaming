from json import dumps
from kafka import KafkaProducer
from rich import print
from time import sleep, time
import random
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

# ====== CONFIG ======
BOOTSTRAP_SERVERS = ["localhost:9092"]   # local Kafka
TOPIC = "my-topic-test"                  # même topic que ton code tweepy
TWEETS_PER_SECOND = 2                    # comme sleep(0.5) => 2 tweets/sec

KEYWORDS = ["music", "tech", "sport", "school", "news"]
LANGS = ["en", "fr", "ar"]
CITIES = [
    "Casablanca, MA", "Rabat, MA", "Marrakech, MA", "Fes, MA", "Tangier, MA",
    "Paris, FR", "London, UK", "New York, US"
]

HASHTAGS_BY_KEYWORD = {
    "music": ["music", "live", "playlist", "song", "concert", "beats"],
    "tech": ["bigdata", "kafka", "flink", "hadoop", "cloud", "dev"],
    "sport": ["football", "match", "goal", "win", "team"],
    "school": ["tp", "student", "exam", "project", "university"],
    "news": ["breaking", "update", "today", "world", "report"]
}
# ====================


def generate_fake_tweet(q_keyword="music"):
    tags = HASHTAGS_BY_KEYWORD.get(q_keyword, ["trending"])
    chosen = random.sample(tags, k=random.randint(1, min(3, len(tags))))

    text_templates = [
        f"{fake.sentence(nb_words=random.randint(6, 12))} " + " ".join("#" + t for t in chosen),
        f"{fake.sentence(nb_words=random.randint(5, 10))} 🔥 " + " ".join("#" + t for t in chosen),
        f"{fake.sentence(nb_words=random.randint(7, 14))} ✅ " + " ".join("#" + t for t in chosen),
    ]
    full_text = random.choice(text_templates)

    created_at = datetime.now(timezone.utc).isoformat()

    cur_data = {
        "id_str": f"{int(time() * 1000)}{random.randint(1000, 9999)}",
        "username": fake.name(),
        "tweet": full_text,
        "location": random.choice(CITIES),
        "created_at": created_at,
        "retweet_count": random.randint(0, 5000),
        "favorite_count": random.randint(0, 20000),
        "followers_count": random.randint(0, 500000),
        "lang": random.choice(LANGS),
        "hashtags": chosen,      # bonus utile
        "query": q_keyword       # bonus utile
    }
    return cur_data


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: dumps(v).encode("utf-8")
    )

    delay = 1.0 / max(1, TWEETS_PER_SECOND)
    print(f"[green]✅ Fake X stream -> Kafka topic '{TOPIC}' on {BOOTSTRAP_SERVERS}[/green]")

    try:
        while True:
            q = random.choice(KEYWORDS)  # comme q='music'
            cur_data = generate_fake_tweet(q_keyword=q)

            producer.send(TOPIC, value=cur_data)
            print(cur_data)
            sleep(delay)

    except KeyboardInterrupt:
        print("\n[yellow]🛑 Arrêt (Ctrl+C).[/yellow]")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
