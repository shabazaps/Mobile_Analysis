from pyspark.sql import SparkSession
from pyspark.sql.functions import when



spark = SparkSession.builder.appName("MobileAnalysis").getOrCreate()
df = spark.read.format("csv").option("header","True").option("inferSchema",True).option("delimiter",",").load('F:\SparkCourse\csv\mobileDataset.csv')


# Replace empty strings with null values
df = df.withColumn("Storage", when(df["Internal_memory"] != "", df["Internal_memory"]).otherwise(None))
df = df.withColumn("BatteryCapacity", when(df["Battery"] != "", df["Battery"]).otherwise(None))
df = df.withColumn("Processor", when(df["CPU"] != "", df["CPU"]).otherwise(None))
df = df.withColumn("Brand", when(df["Brand"] != "", df["Brand"]).otherwise(None))
df = df.withColumn("Network", when(df["Network"] != "", df["Network"]).otherwise(None))
df = df.withColumn("Model", when(df["Model"] != "", df["Model"]).otherwise(None))

# Convert relevant columns to numeric types
df = df.withColumn("Storage", df["Storage"].cast("int"))
df = df.withColumn("BatteryCapacity", df["BatteryCapacity"].cast("int"))

# Filter the dataset to exclude rows with null values
df = df.na.drop(subset=["Brand", "Model", "Storage", "BatteryCapacity", "Processor", "Network"])



from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Define a window specification
window_spec = Window.partitionBy("Operating_System").orderBy(desc("Storage"), desc("BatteryCapacity"), desc("Processor"))

# Add row numbers to the DataFrame based on the window specification
df = df.withColumn("rank", row_number().over(window_spec))

# Filter the top 5 phones for iOS and Android
ios_best_phones = df.filter((df["Operating_System"] == "iOS") & (df["rank"] <= 5))
android_best_phones = df.filter((df["Operating_System"] == "Android") & (df["rank"] <= 5))




#1.Market captured by mobile phone brands according to the number of phone models:
brand_market_share = df.groupBy("Brand").count().withColumnRenamed("count", "ModelCount")



#2.Phone models with 4G or LTE network, and phones with 5G network:
from pyspark.sql.functions import col

# Filter phone models with 4G/LTE and 5G networks
models_with_4g_lte = df.filter(col("Network").rlike(r'4G|LTE'))
models_with_5g = df.filter(col("Network").rlike(r'5G'))


#3.Brand with the most number of models with (LTE, 4G network) or 5G network:
from pyspark.sql.functions import when, count

# Create a column to flag whether a phone has (LTE, 4G network) or 5G network
df = df.withColumn("HasLTE4G", when(col("Network").rlike(r'4G|LTE'), "Yes").otherwise("No"))
df = df.withColumn("Has5G", when(col("Network").rlike(r'5G'), "Yes").otherwise("No"))

# Count the number of models for each brand with (LTE, 4G network) and 5G network
brand_with_most_models_lte = df.groupBy("Brand").agg(count("Model").alias("LTE4GCount")).orderBy(desc("LTE4GCount")).first()
brand_with_most_models_5g = df.groupBy("Brand").agg(count("Model").alias("5GCount")).orderBy(desc("5GCount")).first()


#4.List of discontinued phones released after the year 2015:
from pyspark.sql.functions import year, when

# Extract the year from the "Announced" column
df = df.withColumn("AnnouncedYear", year(df["Announced"]))

# Filter discontinued phones released after 2015
discontinued_phones_after_2015 = df.filter((df["Status"] == "Discontinued") & (df["AnnouncedYear"] > 2015))



export_path = "F:/project/"

brand_market_share.write.csv(export_path + "brand_market_share.csv", header=True, mode="overwrite")
models_with_4g_lte.write(export_path + "models_with_4g_lte.csv", header=True, mode="overwrite")
models_with_5g.write(export_path + "models_with_5g.csv", header=True, mode="overwrite")
brand_with_most_models_lte.write(export_path + "brand_with_most_models_lte.csv", header=True, mode="overwrite")
brand_with_most_models_5g.write(export_path + "brand_with_most_models_5g.csv", header=True, mode="overwrite")
discontinued_phones_after_2015.write(export_path + "discontinued_phones_after_2015.csv", header=True, mode="overwrite")



