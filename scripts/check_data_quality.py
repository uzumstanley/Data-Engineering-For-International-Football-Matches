import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, sum

spark = SparkSession.builder.appName("final_project_check").getOrCreate()

def check_data_quality():
    resultfile_path = "hdfs://localhost:9000/data/international_football_DS/results.csv"
    goalscorersfile_path = "hdfs://localhost:9000/data/international_football_DS/goalscorers.csv"
    shootoutsfile_path = "hdfs://localhost:9000/data/international_football_DS/shootouts.csv"
    
    df_result=spark.read.csv(resultfile_path,header=True,inferSchema=True)
    
    df_goalscorers=spark.read.csv(goalscorersfile_path,header=True,inferSchema=True)
    
    df_shootouts=spark.read.csv(shootoutsfile_path,header=True,inferSchema=True)
    #check null vals
    df_shootouts.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_shootouts.columns]).show()
    #drop null column
    df_shootouts=df_shootouts.drop("first_shooter")
    df_shootouts.show(5)
    #check null vals
    df_result.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_result.columns]).show()
    #check null vals
    df_goalscorers.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_goalscorers.columns]).show()



check_data_quality()