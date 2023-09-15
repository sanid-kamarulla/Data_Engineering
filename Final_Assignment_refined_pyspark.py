# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="/files/images/437beinex_Logo.png" alt="Beinex">
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # <span style="color:blue">Databricks Final Assignment</span>
# MAGIC ### Assigned by Sangeetha Gopinath
# MAGIC
# MAGIC
# MAGIC ######Submitted by: Sanid Kamarulla Sayeed (Trainee in DE)
# MAGIC ######Submission Date: 04 - 07 - 2023

# COMMAND ----------

#file the location of the file 'googleplaystore.csv'
display(dbutils.fs.ls("/FileStore/final_assignment/"))

#create the dataframe the CSV file
apps_df = spark.read.option('header',True)\
                    .option('inferSchema',True)\
                    .csv('dbfs:/FileStore/final_assignment/googleplaystore.csv')

#display the dataframe
display(apps_df.limit(10))

#print the schema
apps_df.printSchema()

# COMMAND ----------

dbutils.data.summarize(apps_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upon primary inspection,
# MAGIC 1. Duplicate app names are present in App Column, App names have unnecessary qoutes
# MAGIC 1. NaN string, abnormal rating present in Rating column
# MAGIC 1. Missing values and NaN strings are present in Current Ver
# MAGIC 1. NaN string are present in Android Ver
# MAGIC 1. Install column, Size column and price column contain charecters like '$',',','+'.
# MAGIC 1. NaN in content rating, Type
# MAGIC 1. Size is not standardised.
# MAGIC 1. Datatype is not properly emforced for all columns(eg:Last Updated).

# COMMAND ----------

#First let us start with the App column. We know that App have duplicate names.Let's view it.
from pyspark.sql.functions import desc
display(apps_df.groupBy("App").count().orderBy(desc('count')))

# COMMAND ----------

#let us view the most repeating one
display(apps_df.filter(apps_df.App == apps_df.groupBy("App").count().orderBy(desc('count')).first()[0]))

#As we can see some apps are duplicated. In the case of 'ROBLOX' the highest duplicated app, only the 'Category' and the 'Reviews' columns are changing in value. Let us assume it is same for all duplicated cases. So let us select those App names where Review count is the highest and ignore the change in Category column.

#let us find those non-integer values in Reviews column
from pyspark.sql.functions import col, regexp_extract
display(apps_df.filter(~col('Reviews').rlike('^-?\\d+$')))

#3 rows are returned, were all of them look bad entries for all columns. Lets set aside these 3 rows and create a new dataframe by casting Reviews to integer
from pyspark.sql.functions import col
apps_df1 = apps_df.filter(col('Reviews').rlike('^-?\\d+$')).withColumn('Reviews', col('Reviews').cast('integer'))
#display(apps_df1)

#Now let us drop the duplicate App names.
unique_apps_df = apps_df1.orderBy('App',desc('Reviews')).dropDuplicates(['App'])
#display(unique_apps_df)

#display(unique_apps_df.groupBy("App").count().orderBy(desc('App')))

# COMMAND ----------

#Now let us see category column
display(unique_apps_df.groupBy('Category').count().orderBy(desc('count')))
display(unique_apps_df.select(unique_apps_df.Category.rlike('[0-9]+')).distinct())
#column looks fine

# COMMAND ----------

#Now let us look at the Rating Column
display(unique_apps_df.groupBy('Rating').count().orderBy(desc('Rating')))

#Rating is between 1-5. There are NaN string values present which we have to deal with.
display(unique_apps_df.filter(unique_apps_df.Rating == 'NaN'))

#replace NaN string values with the median obtained from the normal rating and cast Rating column to Double type
from pyspark.sql.functions import col,expr,desc,when
median = unique_apps_df.filter(col('Rating').rlike('[0-9]+')).selectExpr('percentile_approx(Rating, 0.5)').first()[0]
unique_apps_df1 = unique_apps_df.withColumn('Rating', when(col('Rating').rlike('[0-9]+'), col('Rating')).otherwise(median).cast('Double'))
#dbutils.data.summarize(unique_apps_df1)

# COMMAND ----------

#Now let us look at App,Installs and Price column
display(unique_apps_df1.groupBy('Installs').count().orderBy(desc('count')))
display(unique_apps_df1.groupBy('Price').count().orderBy(desc('count')))

#there are charecters like '+' and ',' present in the Installs. There are $ present in Price. There are '"' present in App. Let us remove those values

from pyspark.sql.functions import col, regexp_replace
unique_apps_df2 = unique_apps_df1.withColumn("Installs", regexp_replace(col("Installs"), "[+,]", "").cast('integer')).withColumn("Price", regexp_replace(col("Price"), "[$]", "").cast('integer')).withColumn("App", regexp_replace(col("App"), '["]', "")).withColumnRenamed("Price","Price in Dollars")
#display(unique_apps_df2.groupBy('Installs').count().orderBy(desc('count')))
#display(unique_apps_df2.groupBy('Price').count().orderBy(desc('count')))
#display(unique_apps_df2.groupBy("App").count().orderBy(desc('count')))
#dbutils.data.summarize(unique_apps_df2)

# COMMAND ----------

#Now let us look at the type column. It should be either Paid or free
display(unique_apps_df2.groupBy('Type').count().orderBy(desc('count')))
#display(unique_apps_df2.filter(col('Type') == "NaN"))

#let us replace this NaN value with the mode value
mode_Type = unique_apps_df2.groupBy('Type').count().orderBy(col('count').desc()).first()[0]
#display(mode_Type)

unique_apps_df3 = unique_apps_df2.withColumn('Type', when((col('Type') == 'Free')| (col('Type') == 'Paid'), col('Type')).otherwise(mode_Type))
#display(unique_apps_df3.groupBy('Type').count().orderBy(desc('count')))

# COMMAND ----------

#Now let us look into current ver and android ver
#dbutils.data.summarize(unique_apps_df3)
display(unique_apps_df3.filter(col("Current Ver").isNull()|(col("Current Ver")=='NaN')))
display(unique_apps_df3.filter(col("Android Ver")=='NaN'))

#there are null values and NaN strings present in the current version. 
#there are NaN strings present Android ver column

#display(unique_apps_df3.groupBy('Current Ver').count().orderBy(desc('count')))
#display(unique_apps_df3.groupBy('Android Ver').count().orderBy(desc('count')))

# let us calculate mode values for both columns. 
mode_Current_Ver = unique_apps_df3.groupBy('Current Ver').count().orderBy(col('count').desc()).first()[0]
mode_Android_Ver = unique_apps_df3.groupBy('Android Ver').count().orderBy(col('count').desc()).first()[0]
#display(mode_Current_Ver,mode_Android_Ver)

#Now let us look into the content rating column
display(unique_apps_df3.groupBy('Content Rating').count().orderBy(desc('count')))
#looks fine.

#Now let us look into the Last Updated column
display(unique_apps_df3.groupBy('Last Updated').count().orderBy(desc('Last Updated')))

#let's cast it to date type since it is string type. Let's replace current ver and android ver with calculate mode values.
from pyspark.sql.functions import to_date
unique_apps_df4 = unique_apps_df3.withColumn("Last Updated", to_date("Last Updated", "MMMM d, yyyy")).withColumn('Current Ver', when((col('Current Ver').isNotNull())|(col('Current Ver') != 'NaN'), col('Current Ver')).otherwise(mode_Current_Ver)).withColumn('Android Ver', when((col('Android Ver') != 'NaN'), col('Android Ver')).otherwise(mode_Android_Ver))

# COMMAND ----------

#Now let us look into the Genres Column
#display(unique_apps_df4.groupby("Genres").count().orderBy('Genres'))
#looks fine. 

# Let's make a list of Genres
from pyspark.sql.functions import split,explode
Genre_List = unique_apps_df4.withColumn("GenresArray", split("Genres", ";")).select(explode("GenresArray").alias("Genres")).distinct().orderBy('Genres')
#Genre_List = unique_apps_df5.select("Genres").distinct().explode("Genres",';')
display(Genre_List)

# COMMAND ----------

#Now finally let us look into size
#display(unique_apps_df3.groupBy('size').count().orderBy(desc('count')))

#Now let us normalise the size column by removing symbols, albhabets to a standardised form of size in MB
# let's create a UDF
def value_to_float(x):
    if type(x) == float or type(x) == int:
        return x
    if 'K' in x:
        if len(x) > 1:
            return float(x.replace('K', '')) * 1000
    if 'M' in x:
        if len(x) > 1:
            return float(x.replace('M', '')) * 1000000
        
#Convert the python function into UDF
from pyspark.sql.types import DoubleType
value_to_float_udf = udf(value_to_float, DoubleType())

final_apps_df = unique_apps_df4.withColumn("Size", (value_to_float_udf("Size")/1000000).cast('string')).withColumnRenamed('Size','Size in MB').fillna({'Size in MB':'Varies with device'})
display(final_apps_df.limit(10))

# COMMAND ----------

dbutils.data.summarize(final_apps_df)

# COMMAND ----------

#The data looks clean now and ready to use
display(final_apps_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Find the top 10 reviews given to the apps

# COMMAND ----------

#display(final_unique_apps_df)
top_reviews = final_apps_df.orderBy(desc('Reviews')).select('App','Reviews')
display(top_reviews.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Top 10 installs apps and distribution of type(free/paid)

# COMMAND ----------

top_installs = final_apps_df.orderBy(desc('Installs')).select('App', 'Installs', 'Type')
display(top_installs.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Q4. Category wise distribution of installed apps.

# COMMAND ----------

Cat_distribution = final_apps_df.groupBy('Category').count().withColumnRenamed('count','Count of Apps').orderBy(desc('Count of Apps'))
display(Cat_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC Q5. Top paid apps.

# COMMAND ----------

top_paid_apps = final_apps_df.orderBy(desc('Price in Dollars')).filter((final_apps_df.Type == 'Paid')).select('App','Price in Dollars')
display(top_paid_apps)

# COMMAND ----------

# MAGIC %md
# MAGIC Q6. Top paid rating apps.

# COMMAND ----------

 top_paid_dating_apps = final_apps_df.orderBy(desc('Rating'),desc('Reviews'),desc('Installs')).filter((final_apps_df.Type == 'Paid') & (final_apps_df.Reviews > 100) & (final_apps_df.Installs > 10000)).select('App','Rating','Reviews','Installs')
display(top_paid_dating_apps)

