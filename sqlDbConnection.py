from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQLServerConnection") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "2") \
    .config("spark.driver.extraClassPath", "source/drivers/mssql-jdbc-12.10.0.jre11.jar") \
    .getOrCreate()


jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=YourDB;encrypt=true;trustServerCertificate=true"
connection_properties = {
    "user": "Username",
    "password": "Password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


df = spark.read.jdbc(
    url=jdbc_url,
    table="TableName",
    properties=connection_properties
)

df.show()
