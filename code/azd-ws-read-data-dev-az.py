# Databricks notebook source
import pyspark.sql.functions  as f
from pyspark.sql.window import Window

# COMMAND ----------

#Connect To Azure DataLakeStorage
spark.conf.set(
            "fs.azure.account.key.sadevdatalakeaz.dfs.core.windows.net",
            "<Generate Token>"
            )

# COMMAND ----------

# COMMAND ----------

#Access the files in the container through the Data Lake Storage Account
dbutils.fs.ls("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/")

# COMMAND ----------

# COMMAND ----------

#Commenting it out because it will be used in other notebook
dbutils.fs.mount(
source = "wasbs://<storage-container-name>@<storage-account-name>.blob.core.windows.net",
mount_point = "/mnt/<storage-container-name>",
extra_configs = {"fs.azure.sas.<storage-container-name>.<storage-account-name>.blob.core.windows.net":"<Genrate Key Token>"})

# COMMAND ----------

# COMMAND ----------

#Unmount a file in the DBFS
dbutils.fs.unmount("/mnt/<storage-container-name>/")

# COMMAND ----------

#List all the mount files present 
dbutils.fs.ls('/mnt/')

# COMMAND ----------

#Read Yelp DataSets in ADLS Gen2 and convert Json to Parquet for better performance
#File Location and Type for movies,link,ratings
business = "/mnt/<storage-container-name>/yelp_dataset/yelp_academic_dataset_business.json"
checkin = "/mnt/<storage-container-name>/yelp_dataset/yelp_academic_dataset_checkin.json"
review = "/mnt/<storage-container-name>/yelp_dataset/yelp_academic_dataset_review.json"
tip = "/mnt/<storage-container-name>/yelp_dataset/yelp_academic_dataset_tip.json"
user = "/mnt/<storage-container-name>/yelp_dataset/yelp_academic_dataset_tip.json"

# COMMAND ----------

df_business = spark.read.json(business)
df_business.write.mode('overwrite').parquet("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/business.parquet")



df_checkin = spark.read.json(checkin)
df_checkin.write.mode('overwrite').parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/checkin.parquet')

df_review = spark.read.json(review)
df_review.write.mode('overwrite').parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/review.parquet')

df_tip = spark.read.json(tip)
df_tip.write.mode('overwrite').parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/tip.parquet')

df_user = spark.read.json(user)
df_user.write.mode('overwrite').parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/user.parquet')

# COMMAND ----------

#Delta is also faster but using Parquet For this Project
#df_user = spark.read.json(user)
#df_user.write.mode('overwrite').parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/user.delta')

# COMMAND ----------

df_business =spark.read.parquet("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/business.parquet")

# COMMAND ----------

df_checkin = spark.read.parquet("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/checkin.parquet")
df_review = spark.read.parquet("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/review.parquet")
df_tip = spark.read.parquet("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/tip.parquet")
df_user = spark.read.parquet("abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/yelp_dataset_json_to_parquet/user.parquet")

# COMMAND ----------

#Total Records in Each datasets
print("df_tip: "+ str(df_tip.count()))
print("df_business: "+ str(df_business.count()))
print("df_checkin: "+ str(df_checkin.count()))
print("df_review: "+ str(df_review.count()))
print("df_user: "+ str(df_user.count()))

# COMMAND ----------

display(df_tip.show(10))

# COMMAND ----------

#Extract Year from Date and Time
df_tip = df_tip.withColumn("tip_year",f.year(f.to_date(f.col("date"))))
df_tip = df_tip.withColumn("tip_month",f.month(f.to_date(f.col("date"))))
display(df_tip)

# COMMAND ----------

#Partition dataset tip by date column
df_tip.write.mode('overwrite').partitionBy("tip_year",                              "tip_month").parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/tip_partitioned_by_year_and_month/')

# COMMAND ----------

#repartition and coalesce
print("Current Number of Partitions " + str(df_user.rdd.getNumPartitions()))

df_reduce_part = df_user.coalesce(10)

print("Reduced Number of Partitions after Coalesce " + str(df_reduce_part.rdd.getNumPartitions()))


df_increase_part = df_user.repartition(30)

print("Increased Number of Partitions after Coalesce " + str(df_increase_part.rdd.getNumPartitions()))

# COMMAND ----------

df_user.coalesce(10).write.mode('overwrite').parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/coalesce/user.parquet')

# COMMAND ----------

df_user.repartition(10).write.mode('overwrite').parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/repartition/user.parquet')

# COMMAND ----------

#Reading Dataset with repartitioning
df_user  = spark.read.parquet('abfss://<storage-container-name>@<storage-account-name>.dfs.core.windows.net/repartition/user.parquet')
display(df_user)

# COMMAND ----------

df_user.createOrReplaceTempView("v_user")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC     user_id,
# MAGIC     name,
# MAGIC     review_count
# MAGIC FROM 
# MAGIC     v_user
# MAGIC ORDER BY 
# MAGIC     review_count DESC  ---Find the top 3 users based on their total number of reviews
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   user_id,
# MAGIC   name,
# MAGIC   fans
# MAGIC FROM
# MAGIC   v_user
# MAGIC ORDER BY
# MAGIC   fans DESC ---Find the top 10 users with the most fans
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

display(df_business)

# COMMAND ----------

#Analyse Top 10 Categories by the number of reviews
df_business_cat = df_business.groupBy("categories").agg(f.count("review_count").alias("total_review_count"))
df_top_categories = df_business_cat.withColumn('rnk',f.row_number().over(Window.orderBy(f.col('total_review_count').desc())))
df_top_categories = df_top_categories.filter(f.col('rnk') <= 10)
display(df_top_categories)

# COMMAND ----------

df_business.createOrReplaceTempView("v_business")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   categories,
# MAGIC   total_review_count
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       business_categories.*,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         ORDER BY
# MAGIC           total_review_count DESC
# MAGIC       ) AS rn
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           categories,
# MAGIC           COUNT(review_count) AS total_review_count
# MAGIC         FROM
# MAGIC           v_business
# MAGIC         GROUP BY
# MAGIC           categories
# MAGIC       ) business_categories
# MAGIC   )
# MAGIC WHERE
# MAGIC   rn <= 10

# COMMAND ----------

#Analyze Top Business which have over 1000 reviews 
df_business_reviews = df_business.groupBy("categories").agg(f.count("review_count").alias("total_review_count"))
df_top_business = df_business_reviews.filter(df_business_reviews['total_review_count'] >= 1000).orderBy(f.desc('total_review_count'))

# COMMAND ----------

display(df_top_business)

# COMMAND ----------

#Analyze Business Data : Number of restaurants per state
df_num_of_restaurants = df_business.select('state').groupBy('state').count().orderBy(f.desc('count'))
display(df_num_of_restaurants)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       STATE,
# MAGIC       name,
# MAGIC       review_count,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY STATE
# MAGIC         ORDER BY
# MAGIC           review_count DESC
# MAGIC       ) rn
# MAGIC     FROM
# MAGIC       v_business
# MAGIC   )
# MAGIC WHERE
# MAGIC   rn <= 3 --#Analyze top 3 restaurants in each state

# COMMAND ----------

#Number of Restaurants in Arizona State
df_business_az = df_business.filter(df_business['state'] == 'AZ')
df_business_az = df_business_az.groupBy('city').count().orderBy(f.desc('count'))

# COMMAND ----------

display(df_business_az)

# COMMAND ----------

#Most Rated Italian Restaurant in Phoenix
df_business_px = df_business.filter(df_business.city == 'Tucson')
df_business_italian = df_business_px.filter(df_business.categories.contains('Italian'))

# COMMAND ----------

display(df_business_px)

# COMMAND ----------

df_best_italian_rest = df_business_italian.groupBy("name").agg(f.count("review_count").alias('review_count'))
df_best_italian_rest = df_best_italian_rest.filter(df_best_italian_rest['review_count'] >= 5)

# COMMAND ----------

display(df_best_italian_rest)

# COMMAND ----------

