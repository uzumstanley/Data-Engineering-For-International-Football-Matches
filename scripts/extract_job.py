import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

spark = SparkSession.builder.appName("final_project_extract").getOrCreate()

#Reading Files From HDFS Local
def extract_data():
    resultfile_path = "hdfs://localhost:9000/data/international_football_DS/results.csv"
    goalscorersfile_path = "hdfs://localhost:9000/data/international_football_DS/goalscorers.csv"
    shootoutsfile_path = "hdfs://localhost:9000/data/international_football_DS/shootouts.csv"
    
    df_result=spark.read.csv(resultfile_path,header=True,inferSchema=True)
    
    df_goalscorers=spark.read.csv(goalscorersfile_path,header=True,inferSchema=True)
    
    df_shootouts=spark.read.csv(shootoutsfile_path,header=True,inferSchema=True)
    
    df_result.show(5)
    df_goalscorers.show(5)
    df_shootouts.show(5)

extract_data()