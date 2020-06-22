from pyspark.sql.functions import lit, when, col
import pyspark.sql.functions as f
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

df_sexe = spark.read.option("header", "true").option("delimiter", ";").csv(
    "donnees-hospitalieres-covid19-2020-06-18-19h00.csv")
df_etab = spark.read.option("header", "true").option("delimiter", ";").csv(
    "donnees-hospitalieres-etablissements-covid19-2020-06-19-19h00.csv")
"""
df1 = df_sexe.withColumn("TotalHosp", lit(None)).withColumn("TotalRea", lit(None)).withColumn("TotalRad", lit(None)) \
    .withColumn("TotalDc", lit(None))
df2 = df_sexe.withColumn("HHosp", lit(None)).withColumn("HRea", lit(None)).withColumn("HRad", lit(None)) \
    .withColumn("HDc", lit(None))
df3 = df_sexe.withColumn("FHosp", lit(None)).withColumn("FRea", lit(None)).withColumn("FRad", lit(None)) \
    .withColumn("FDc", lit(None))
"""


df1 = df_sexe.withColumn("TotalHosp", df_sexe.hosp)\
    .withColumn("TotalRea", df_sexe.rea)\
    .withColumn("TotalRad", df_sexe.rad) \
    .withColumn("TotalDc", df_sexe.dc)\
    .where(f.col('sexe') == 0)
df1 = df1.drop('sexe','hosp','rea','rad','dc')

df2 = df_sexe.withColumn("HHosp", df_sexe.hosp)\
    .withColumn("HRea", df_sexe.rea)\
    .withColumn("HRad", df_sexe.rad) \
    .withColumn("HDc", df_sexe.dc)\
    .where(f.col('sexe') == 1)
df2 = df2.drop('sexe','hosp','rea','rad','dc')

df3 = df_sexe.withColumn("FHosp", df_sexe.hosp)\
    .withColumn("FRea", df_sexe.rea)\
    .withColumn("FRad", df_sexe.rad) \
    .withColumn("FDc", df_sexe.dc)\
    .where(f.col('sexe') == 2)
df3 = df3.drop('sexe','hosp','rea','rad','dc')

df1.show()
df2.show()
df3.show()

cond1 = [df1.dep == df2.dep, df1.jour == df2.jour]
df_join1 = df1.join(df2, cond1, how='left').drop(df2.dep).drop(df2.jour)
cond2 = [df_join1.dep == df3.dep,df_join1.jour == df3.jour]
df_join2 = df_join1.join(df3, cond2, how='left').drop(df3.dep).drop(df3.jour)

cond3 = [df_join2.dep == df_etab.dep, df_join2.jour == df_etab.jour]
df_join3 = df_join2.join(df_etab, cond3, how='left').drop(df_etab.dep).drop(df_etab.jour)

df_join3.show()

df_join3.write.parquet("result","overwrite")
res = spark.read.parquet("result")
res.show()
