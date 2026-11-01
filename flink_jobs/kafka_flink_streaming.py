import os
from pathlib import Path

# Set FLINK_LIB_DIR to the directory containing your connector JARs.
CONNECTOR_DIR = Path(os.environ.get(
    "FLINK_LIB_DIR", Path(__file__).resolve().parent.parent / "connectors"
)).expanduser().resolve()

from pyflink.table import TableEnvironment, EnvironmentSettings

# Create a TableEnvironment (streaming mode)
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Specify connector jar (chemin réel chez toi)
t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    (CONNECTOR_DIR / "flink-sql-connector-kafka_2.11-1.14.4.jar").as_uri()
)

# Define source table DDL
source_ddl = """
CREATE TABLE source_table (
    id_str STRING,
    username STRING,
    tweet STRING,
    location STRING,
    retweet_count BIGINT,
    followers_count BIGINT,
    lang STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'my-topic-test',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'test_3',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
)
"""

# Execute DDL statement
t_env.execute_sql(source_ddl)

# Retrieve the source table
source_table = t_env.from_path('source_table')

print("Source Table Schema:")
source_table.print_schema()

# SQL query
sql_query = "SELECT * FROM source_table"

# Execute the query
result_table = t_env.sql_query(sql_query)

# Print results to console
result_table.execute().print()
