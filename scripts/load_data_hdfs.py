from transform_starschema_job import trasnform_dims_and_facts

df_teams_dim, df_date_dim, df_tournaments_dim, df_locations_dim, df_players_dim, df_fact_matches, df_fact_goalscorers, df_fact_shootouts=trasnform_dims_and_facts()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("load_to_hdfs").getOrCreate()

def load_data_hdfs():
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    
    df_fact_matches.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/fact_matches.parquet")
    df_fact_goalscorers.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/fact_goalscorers.parquet")
    df_fact_shootouts.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/fact_shootouts.parquet")
    df_teams_dim.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/dim_teams.parquet")
    df_date_dim.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/dim_date.parquet")
    df_players_dim.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/dim_players.parquet")
    df_tournaments_dim.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/dim_tournaments.parquet")
    df_locations_dim.write.mode("overwrite").parquet("hdfs://localhost:9000/parquet_data/dim_locations.parquet")

load_data_hdfs()