import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, date_format,sha2, concat_ws

spark = SparkSession.builder.appName("final_project_transform_dims").getOrCreate()


def trasnform_dims_and_facts():
    resultfile_path = "hdfs://localhost:9000/data/international_football_DS/results.csv"
    goalscorersfile_path = "hdfs://localhost:9000/data/international_football_DS/goalscorers.csv"
    shootoutsfile_path = "hdfs://localhost:9000/data/international_football_DS/shootouts.csv"
    
    df_result=spark.read.csv(resultfile_path,header=True,inferSchema=True)
    
    df_goalscorers=spark.read.csv(goalscorersfile_path,header=True,inferSchema=True)
    
    df_shootouts=spark.read.csv(shootoutsfile_path,header=True,inferSchema=True)

     # create Dim_Teams
    df_teams_dim = df_result.selectExpr("home_team as team").union(df_result.selectExpr("away_team as team")).distinct()
    df_teams_dim = df_teams_dim.withColumn("team_id", monotonically_increasing_id())
    #create dim date
    df_date_dim = df_result.select("date").distinct()
    df_date_dim = df_date_dim.withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
    # Dim_tournament
    df_tournaments_dim = df_result.selectExpr("tournament as tournament").distinct()
    df_tournaments_dim= df_tournaments_dim.withColumn("tournament_id",monotonically_increasing_id())
    # Dim_locations
    df_locations_dim = df_result.select("country","city").distinct()
    df_locations_dim = df_locations_dim.withColumn("location_id",monotonically_increasing_id())
    #create players dim
    df_players_dim = df_goalscorers.selectExpr("scorer as player_name").distinct()
    df_players_dim = df_players_dim.withColumn("player_id",monotonically_increasing_id())

    
    ##create matches Fact--------------------
    
    #  match_id Hash 
    df_fact_matches = df_result.withColumn(
        "match_id",
        sha2(concat_ws("_", col("date"), col("home_team"), col("away_team"), col("tournament")), 256)
    )
    
    # Dim_Teamر
    df_fact_matches = df_fact_matches \
        .join(df_teams_dim.withColumnRenamed("team", "home_team").withColumnRenamed("team_id", "home_team_id"), "home_team", "left") \
        .join(df_teams_dim.withColumnRenamed("team", "away_team").withColumnRenamed("team_id", "away_team_id"), "away_team", "left")
    
    # join with Dim_Tournaments
    df_fact_matches = df_fact_matches.join(df_tournaments_dim,"tournament","left")
    # join with Dim_Locations
    df_fact_matches = df_fact_matches.join(df_locations_dim, ["city", "country"] ,"left")
    # join with Dim_date
    df_fact_matches = df_fact_matches.join(df_date_dim,"date" ,"left")
    #select cols
    df_fact_matches=df_fact_matches.select("match_id","date_id","home_team_id","away_team_id","tournament_id","home_score","away_score","location_id","neutral")


    

    #create fact goalscorers
    
    df_fact_goalscorers = df_goalscorers \
        .join(df_teams_dim.withColumnRenamed("team", "home_team")
                          .withColumnRenamed("team_id", "home_team_id"), "home_team", "left") \
        .join(df_teams_dim.withColumnRenamed("team", "away_team")
                          .withColumnRenamed("team_id", "away_team_id"), "away_team", "left") \
        .join(df_players_dim.withColumnRenamed("player_name","scorer").withColumnRenamed("player_id","player_scorer_id"),"scorer","left") \
    
    
    df_fact_goalscorers=df_fact_goalscorers.join(df_date_dim,"date","left")
    
    df_fact_goalscorers=df_fact_goalscorers.select("date_id", "home_team_id", "away_team_id","team", "player_scorer_id", "minute", "own_goal", "penalty")

    # create shootouts fact
    
    df_fact_shootouts = df_shootouts \
    .join(df_teams_dim.withColumnRenamed("team","home_team").withColumnRenamed("team_id","home_team_id"),"home_team","left") \
    .join(df_teams_dim.withColumnRenamed("team","away_team").withColumnRenamed("team_id","away_team_id"),"away_team","left") \
    .join(df_date_dim ,"date","left")
    
    df_fact_shootouts=df_fact_shootouts.select("date_id","home_team_id","away_team_id","winner")

    return df_teams_dim, df_date_dim, df_tournaments_dim, df_locations_dim, df_players_dim, df_fact_matches, df_fact_goalscorers, df_fact_shootouts


    


trasnform_dims_and_facts()
