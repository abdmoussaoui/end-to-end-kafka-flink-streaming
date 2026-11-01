import os
from pathlib import Path

# Set FLINK_LIB_DIR to the directory containing your connector JARs.
CONNECTOR_DIR = Path(os.environ.get(
    "FLINK_LIB_DIR", Path(__file__).resolve().parent.parent / "connectors"
)).expanduser().resolve()

from pyflink.table import TableEnvironment, EnvironmentSettings

# Create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Specify connector and format jars (chemins rels)
t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    ";".join((CONNECTOR_DIR / name).as_uri() for name in (
        "flink-sql-connector-kafka_2.11-1.14.4.jar",
        "flink-sql-connector-elasticsearch7_2.11-1.14.4.jar",
    ))
)

# Define source table DDL (Kafka local)
source_ddl = """
    CREATE TABLE source_table(
        id_str VARCHAR,
        username VARCHAR,
        tweet VARCHAR,
        location VARCHAR,
        retweet_count BIGINT,
        followers_count BIGINT,
        lang VARCHAR
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'my-topic-test',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'test_3',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""

# Define sink table DDL (Elasticsearch local)
sink_ddl = """
    CREATE TABLE sink_table(
        id_str VARCHAR,
        username VARCHAR,
        tweet VARCHAR,
        location VARCHAR,
        retweet_count BIGINT,
        followers_count BIGINT,
        lang VARCHAR
    ) WITH (        
        'connector' = 'elasticsearch-7',
        'index' = 'demo_kafka_flink_streaming',
        'hosts' = 'http://localhost:9200',
        'format' = 'json'
    )
"""

# Execute DDL statements to create tables
t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

# Retrieve the source table
source_table = t_env.from_path('source_table')

print("Source Table Schema:")
source_table.print_schema()

# Process the data (inchang)
result_table = source_table.select("*")

# Retrieve the sink table
sink_table = t_env.from_path('sink_table')

print("Sink Table Schema:")
sink_table.print_schema()

# Insert the processed data into the sink table
table_result = result_table.execute_insert('sink_table')

#  IMPORTANT: garder le job vivant (sinon 0 job)
table_result.get_job_client().get_job_execution_result().result()
