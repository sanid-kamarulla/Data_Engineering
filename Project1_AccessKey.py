# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div align="center">
# MAGIC   <img src="/files/images/437beinex_Logo.png" alt="Beinex">
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC # <span style="color:#0F3B5E ;">Databricks Project 1</span>
# MAGIC ### Assigned by Muhammed Roshan
# MAGIC
# MAGIC
# MAGIC ######Submitted by: Sanid Kamarulla Sayeed (Trainee in DE)
# MAGIC ######Submission Date: 24 - 07 - 2023
# MAGIC ---
# MAGIC ##### Project Description
# MAGIC The department store has online e-commerce platform for selling furniture, office supplies and accessories. There are four regions which are in Silos due to different types of systems. However, the data collected are more or less similar, the format of the data and type of the data may be different in each region.
# MAGIC
# MAGIC The end users are looking for two datasets:
# MAGIC 1. Overall sales with returns
# MAGIC 1. A comparison data: yearly quota by region vs actual sales for region in each year
# MAGIC ---
# MAGIC #####Pre-processing
# MAGIC All files used in the project are mounted to the DBFS location `dbfs:/mnt/Project1/` inside a folder named **`Use_Case_ADF`** from an ADLS Gen2 Container named **`row`** from the storage account **`destinationde2023sanidbe`**.
# MAGIC
# MAGIC Before uploading the files, a thorough examination is carried out to study the data. It has been found that the Product name column in the data files ('Product name' may vary across different files) contains the ',' charecter, which is the delimiter of the csv file. To overcome this issue, in all csv files, delimiter is changed to '|'. Another option is to qoute.
# MAGIC
# MAGIC As mentioned in the description there are different files with sales information from four different regions, namely, Central, East, South and West. Central, East and West have one file each, and South have partitioned data based on the year of sales taken place. There are two more files with return information and quoata information which we will discuss along.

# COMMAND ----------

#unmount the ADLS Gen2 Container in DBFS
#dbutils.fs.unmount("/mnt/Project1")

#mount the ADLS Gen2 Container in DBFS
dbutils.fs.mount(
                 source = 'wasbs://row@destinationde2023sanidbe.blob.core.windows.net/',
                 mount_point = '/mnt/Project1',
                 extra_configs = {'fs.azure.account.key.destinationde2023sanidbe.blob.core.windows.net':'ldLxFkYC3IxZtbkyHwXbQKp6ZqatWxabTMtLsYC7iRrFQrAX1ZSj7nrPK389dBjrvnqsrkLPRTjT+AStk9V3DA=='})

# COMMAND ----------

#let's have a look at our files
display(dbutils.fs.ls('/mnt/Project1/Use_Case_ADF'));

#let us first consider the data files from south region
display(dbutils.fs.ls('/mnt/Project1/Use_Case_ADF/South/'));

#let's start by creating temp views from the files
# Define a list of years for which we want to create temporary views

years = [2015, 2016, 2017, 2018];

# Loop through the list of years to create different temp views for each year
for year in years:
    # Create a temporary view for each year
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW orders_south_temp_view_{year}
    USING CSV
    OPTIONS(
        path = 'dbfs:/mnt/Project1/Use_Case_ADF/South/orders_south_{year}.csv',
        header = 'True',
        inferSchema = 'True',
        delimiter = '|'
    )
    """);

# COMMAND ----------

# MAGIC %sql
# MAGIC --Now that we have temp views from all year, combine all to create a single temp view.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_South_temp_view AS
# MAGIC     SELECT * FROM orders_south_temp_view_2015
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM orders_south_temp_view_2016
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM orders_south_temp_view_2017
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM orders_south_temp_view_2018;
# MAGIC
# MAGIC --display the data from south
# MAGIC SELECT * FROM Orders_South_temp_view
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The `Order Date` and `Ship Date` have different date formats and the columns are of string type. Let's create a SQL UDF to convert the date to a standard format, i.e 'yyyy/MM/dd'
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION standard_date(name STRING)
# MAGIC   RETURNS DATE
# MAGIC   RETURN CASE 
# MAGIC     WHEN name LIKE '%-%' THEN to_date(to_date(name,'d-M-y H:m'), 'yyyy-MM-dd')
# MAGIC     WHEN name LIKE '%/%' THEN to_date(to_date(name,'M/d/yyyy H:mm'), 'yyyy-MM-dd')
# MAGIC     ELSE NULL
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Now create a new temp view with two new columns to the south data temp view, such that `Order Date` and `Ship Date` have standardised date type.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_South_temp_view1 AS
# MAGIC   SELECT *, 
# MAGIC   standard_date(`Order Date`) AS OrderDate,
# MAGIC   standard_date(`Ship Date`) AS ShipDate 
# MAGIC   FROM orders_south_temp_view;
# MAGIC
# MAGIC -- Display the new temp view with two new columns.
# MAGIC SELECT * FROM Orders_South_temp_view1
# MAGIC LIMIT 10;

# COMMAND ----------

#Now let us inspect the data file from East
display(dbutils.fs.ls('/mnt/Project1/Use_Case_ADF/East'));

# COMMAND ----------

# MAGIC %md
# MAGIC The data file is in `.xlsx` extension. To read such files we require external libraries. 
# MAGIC
# MAGIC 1. Click on the "Clusters" tab in the sidebar.
# MAGIC 1. Select the cluster that we are using or want to use.
# MAGIC 1. Click on the "Libraries" tab within the cluster configuration.
# MAGIC 1. Click on the "Install New" button.
# MAGIC 1. In the "Library Source" dropdown, choose "Maven Coordinate."
# MAGIC 1. In the "Maven Coordinate" field, enter the following Maven coordinate for spark-excel: `com.crealytics:spark-excel_2.12:0.13.7`
# MAGIC 1. Click "Install" to add the library to our cluster.
# MAGIC 1. Restart the cluster

# COMMAND ----------

# MAGIC %python
# MAGIC # Read the .xlsx file using Databricks DataFrame API
# MAGIC
# MAGIC Orders_East_df = spark.read.format("com.crealytics.spark.excel") \
# MAGIC             .option("header", "true") \
# MAGIC             .option("inferSchema", "true") \
# MAGIC             .load("dbfs:/mnt/Project1/Use_Case_ADF/East/Orders_East.xlsx")
# MAGIC
# MAGIC #Store the DataFrame as a temp view
# MAGIC Orders_East_df.createOrReplaceTempView('Orders_East_temp_view')
# MAGIC
# MAGIC #Display the temp view of East data
# MAGIC display(Orders_East_df.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change discount type to String
# MAGIC -- Remove any characters from Sales Column (eg: USD 2000 to 2000) and retain sales column as Decimal with 2 decimal places.
# MAGIC -- change `Order Date` and `Ship Date` type to Date from timestamp to standardise it.
# MAGIC -- add these changes as new columns and create a new view.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_East_temp_view1 AS
# MAGIC     SELECT *,
# MAGIC         round(REGEXP_REPLACE(Sales, '[^0-9.]', ''),2) AS sales_new,
# MAGIC         to_date(`Order Date`, 'yyyy-MM-dd') AS OrderDate,
# MAGIC         to_date(`Ship Date`, 'yyyy-MM-dd') AS ShipDate
# MAGIC         FROM Orders_East_temp_view;
# MAGIC
# MAGIC -- Display the new temp view.
# MAGIC SELECT * FROM Orders_East_temp_view1
# MAGIC LIMIT 10;

# COMMAND ----------

#Now let us examine the data from west
display(dbutils.fs.ls('dbfs:/mnt/Project1/Use_Case_ADF/West/'));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view to store the data from West with the following options.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_West_temp_view
# MAGIC     USING CSV
# MAGIC     OPTIONS(
# MAGIC         path = 'dbfs:/mnt/Project1/Use_Case_ADF/West/Orders_West.csv',
# MAGIC         header = 'True',
# MAGIC         inferSchema = 'True',
# MAGIC         delimiter = '|'
# MAGIC         );
# MAGIC
# MAGIC -- Display the temp view
# MAGIC SELECT * FROM Orders_West_temp_view
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- select count * from Orders_West_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Instead full name, states are represented in 2 digit code in this data. let's map these to state names using a SQL UDF given as below.
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION state_name(name STRING)
# MAGIC   RETURNS STRING
# MAGIC   RETURN CASE 
# MAGIC     WHEN name = "AZ" THEN "Arizona"
# MAGIC     WHEN name = "CA" THEN "California"
# MAGIC     WHEN name = "CO" THEN "Colorado"
# MAGIC     WHEN name = "ID" THEN "Idaho"
# MAGIC     WHEN name = "MT" THEN "Montana"
# MAGIC     WHEN name = "NM" THEN "New Mexico"
# MAGIC     WHEN name = "OR" THEN "Oregon"
# MAGIC     WHEN name = "UT" THEN "Utah"
# MAGIC     WHEN name = "WT" THEN "Washington"
# MAGIC     ELSE concat(name, "is not found")
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a new temp view with
# MAGIC -- a column with the new state names
# MAGIC -- standardised date type columns for `Order Date` and `Ship Date` from UDF used before.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_West_temp_view1 AS
# MAGIC       (SELECT *,
# MAGIC       state_name(state) AS State_name,
# MAGIC       standard_date(`Order Date`) AS OrderDate,
# MAGIC       standard_date(`Ship Date`) AS ShipDate 
# MAGIC       FROM Orders_West_temp_view);
# MAGIC
# MAGIC  -- Display the new temp view     
# MAGIC SELECT * FROM Orders_West_temp_view1
# MAGIC LIMIT 10;

# COMMAND ----------

#Now let us examine the data from Central region
display(dbutils.fs.ls('dbfs:/mnt/Project1/Use_Case_ADF/Central/'));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view to store the data from Central with the following options.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_Central_temp_view
# MAGIC     USING CSV
# MAGIC     OPTIONS(
# MAGIC         path = 'dbfs:/mnt/Project1/Use_Case_ADF/Central/Orders_Central.csv',
# MAGIC         header = 'True',
# MAGIC         inferSchema = 'True',
# MAGIC         delimiter = '|'
# MAGIC         );
# MAGIC
# MAGIC -- Display the new temp view
# MAGIC SELECT * FROM Orders_Central_temp_view
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new temp view with new columns for
# MAGIC -- a static value as Central
# MAGIC -- Combining Order Day, Order Month, Order year to create Order Date field and cast the type as Date
# MAGIC -- Combining Ship Day, Ship Month, Ship year to create Ship Date field and cast the type as Date
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_Central_temp_view1 AS
# MAGIC         SELECT *,
# MAGIC                 'Central' AS Region,
# MAGIC                 make_date(`Order Year`,`Order Month`,`Order Day`) AS OrderDate,
# MAGIC                 make_date(`Ship Year`,`Ship Month`,`Ship Day`) AS ShipDate
# MAGIC         FROM Orders_Central_temp_view;
# MAGIC
# MAGIC -- Display the new temp view
# MAGIC SELECT * FROM Orders_Central_temp_view1
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create CTE from new temp views of South, East, West and Central Data with the following 21 columns
# MAGIC --> (Sales, Quantity, Profit, Discount, Region, State, `Row ID`,`Order ID`, `Order Date`,  `Ship Date`, `Ship Mode`, `Customer ID`, `Customer Name`, Segment, Country, City, `Postal Code`, `Product ID`, Category, `Sub-Category`, `Product Name`)
# MAGIC -- Union all CTEs and create temp view for orders
# MAGIC
# MAGIC --Create Orders raw temp view
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_temp_view AS(
# MAGIC
# MAGIC   -- Create CTE for South region Data.
# MAGIC   -- Rename OrderDate AS `Order Date`, ShipDate AS  `Ship Date`
# MAGIC
# MAGIC   WITH Orders_South_CTE AS(
# MAGIC     SELECT Sales, Quantity, Profit, Discount, Region, State, `Row ID`,`Order ID`, OrderDate AS `Order Date`, ShipDate AS  `Ship Date`, `Ship Mode`, `Customer ID`, `Customer Name`, Segment, Country, City, `Postal Code`, `Product ID`, Category, `Sub-Category`, `Product Name`
# MAGIC     FROM Orders_South_temp_view1
# MAGIC   ),
# MAGIC
# MAGIC   -- Create CTE for East region Data.
# MAGIC   -- Rename sales_new AS Sales, Discount_string AS Discount
# MAGIC   -- Rename OrderDate AS `Order Date`, ShipDate AS  `Ship Date`
# MAGIC
# MAGIC   Orders_East_CTE AS(
# MAGIC     SELECT sales_new AS Sales, Quantity, Profit, Discount, Region, State, `Row ID`,`Order ID`, OrderDate AS `Order Date`, ShipDate AS  `Ship Date`, `Ship Mode`, `Customer ID`, `Customer Name`, Segment, Country, City, `Postal Code`, `Product ID`, Category, `Sub-Category`, `Product Name` 
# MAGIC     FROM  Orders_East_temp_view1
# MAGIC   ),
# MAGIC
# MAGIC   -- Create CTE for West region Data.
# MAGIC   -- Rename State_name AS State
# MAGIC   -- Rename OrderDate AS `Order Date`, ShipDate AS  `Ship Date`
# MAGIC
# MAGIC   Orders_West_CTE AS(
# MAGIC     SELECT Sales, Quantity, Profit, Discount, Region, State_name AS State, `Row ID`,`Order ID`, OrderDate AS `Order Date`, ShipDate AS `Ship Date`, `Ship Mode`, `Customer ID`, `Customer Name`, Segment, Country, City, `Postal Code`, `Product ID`, Category, `Sub-Category`, `Product Name`
# MAGIC     FROM Orders_West_temp_view1  
# MAGIC   ),
# MAGIC
# MAGIC   -- Create CTE for Central region Data.
# MAGIC   -- Rename Discounts AS Discount, Product AS `Product Name`
# MAGIC   -- Rename OrderDate AS `Order Date`, ShipDate AS  `Ship Date`
# MAGIC
# MAGIC   Orders_Central_CTE AS(
# MAGIC     SELECT Sales, Quantity, Profit, Discounts AS Discount, Region, State, `Row ID`,`Order ID`, OrderDate AS `Order Date`, ShipDate AS `Ship Date`, `Ship Mode`, `Customer ID`, `Customer Name`, Segment, Country, City, `Postal Code`, `Product ID`, Category, `Sub-Category`, Product AS `Product Name`
# MAGIC     FROM Orders_Central_temp_view1
# MAGIC   )
# MAGIC
# MAGIC   -- Union all CTEs
# MAGIC
# MAGIC   SELECT * FROM Orders_South_CTE
# MAGIC   UNION
# MAGIC   SELECT * FROM Orders_East_CTE
# MAGIC   UNION
# MAGIC   SELECT * FROM Orders_West_CTE
# MAGIC   UNION
# MAGIC   SELECT * FROM Orders_Central_CTE);
# MAGIC
# MAGIC -- Display the Orders raw temp view
# MAGIC
# MAGIC SELECT * FROM Orders_temp_view
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for null values
# MAGIC
# MAGIC SELECT COUNT(*), count(Sales), count(Quantity), count(Profit), count(Discount), count(Region), count(State), count(`Row ID`),count(`Order ID`), count(`Order Date`), count(`Ship Date`), count(`Ship Mode`), count(`Customer ID`), count(`Customer Name`), count(Segment), count(Country), count(City), count(`Postal Code`), count(`Product ID`), count(Category), count(`Sub-Category`), count(`Product Name`)
# MAGIC FROM Orders_temp_view;
# MAGIC
# MAGIC -- None are present in Postal Code

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Discount column is of string type due to the presence of string None.
# MAGIC
# MAGIC SELECT * FROM Orders_temp_view
# MAGIC WHERE `Discount` = 'None'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's update it to 0. And save it to a new temp view.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders AS(
# MAGIC   SELECT
# MAGIC   Sales, Quantity, Profit, 
# MAGIC   CASE
# MAGIC   WHEN Discount NOT RLIKE '^[0-9].' THEN 0 ELSE CAST(Discount AS DOUBLE) END AS
# MAGIC   Discount, 
# MAGIC   Region, State, `Row ID`,`Order ID`, `Order Date`,  `Ship Date`, `Ship Mode`, `Customer ID`, `Customer Name`, Segment, Country, City, `Postal Code`, `Product ID`, Category, `Sub-Category`, `Product Name`
# MAGIC   FROM Orders_temp_view
# MAGIC );
# MAGIC
# MAGIC -- Display the new temp view
# MAGIC SELECT * FROM Orders
# MAGIC LIMIT 10;

# COMMAND ----------

# Now that our Orders Data is ready let us examine the return data. The source data is with .xlsx extension.
display(dbutils.fs.ls('dbfs:/mnt/Project1/Use_Case_ADF'));

# Read the XLSX file using Databricks DataFrame API

return_reasons_new_df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("dbfs:/mnt/Project1/Use_Case_ADF/return reasons_new.xlsx")

#Save the dataframe to a temp view
return_reasons_new_df.createOrReplaceTempView('return_reasons_new_temp_view')

#Display the data on returns
display(return_reasons_new_df.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Split column Notes to 2 fields (separation is “-“). The first part of the split is reason for return and Second part is Approver.
# MAGIC -- Remove field Notes.
# MAGIC -- Save the results to a new temp view.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Returns AS(
# MAGIC   SELECT 
# MAGIC     `Row ID`, `Order Date`, `Order ID`, `Product ID`,`Sub-Category`, Manufacturer, `Product Name`, `Return Reason`,
# MAGIC     SPLIT(Notes, '-')[0] AS description_for_return,
# MAGIC     SPLIT(Notes, '-')[1] AS Approver
# MAGIC   FROM
# MAGIC     return_reasons_new_temp_view);
# MAGIC
# MAGIC -- Display the new temp view
# MAGIC SELECT * FROM Returns
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- count the number of returned products.
# MAGIC
# MAGIC select count(*) from Returns;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join Orders Data with Return data on Product ID and Order ID and store it in a new temp view.
# MAGIC -- Do this becuase there are same Product ID from different Order ID.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_and_Returns_raw AS(
# MAGIC     SELECT o.*,r.`Product ID` AS `Return Product ID`, r.`Order ID` AS `Return Order ID`, r.Manufacturer, r.`Return Reason`, r.description_for_return, r.Approver
# MAGIC     FROM Orders AS o
# MAGIC     LEFT JOIN Returns AS r
# MAGIC     ON o.`Product ID` = r.`Product ID`  AND o.`Order ID` = r.`Order ID`);
# MAGIC
# MAGIC -- Display the Orders_and_returns_raw
# MAGIC SELECT * FROM Orders_and_Returns_raw
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- count the number of joined returns.
# MAGIC
# MAGIC SELECT count(*) FROM Orders_and_Returns_raw
# MAGIC WHERE `Return Order ID` IS NOT NULL and `Return Product ID` IS NOT NULL;
# MAGIC
# MAGIC -- Display return items that are not joined with returns. Total 3.
# MAGIC
# MAGIC SELECT r.`Order ID` AS `Return Order ID`,r.`Product ID` AS `Return Product ID`,o.`Product ID`,o.`Order ID`
# MAGIC FROM Returns AS r
# MAGIC LEFT JOIN Orders o
# MAGIC on r.`Order ID` = o.`Order ID` and r.`Product ID` = o.`Product ID`
# MAGIC WHERE o.`Order ID` IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now that we have temp view of Orders and Returns data we need to update it with sales, profit and discount as 0 for the items returned.
# MAGIC -- Save the result in a new temp file
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Orders_and_Returns AS(
# MAGIC   SELECT  
# MAGIC   CASE WHEN `Return Product ID` IS NOT NULL THEN 0 ELSE Sales END AS Sales, 
# MAGIC   Quantity, 
# MAGIC   CASE WHEN `Return Product ID` IS NOT NULL THEN 0 ELSE Profit END AS Profit, 
# MAGIC   CASE WHEN `Return Product ID` IS NOT NULL THEN 0 ELSE Discount END AS Discount,
# MAGIC   Region, State, `Row ID`,`Order ID`, `Order Date`,  `Ship Date`, `Ship Mode`, `Customer ID`, `Customer Name`, Segment, Country, City, `Postal Code`, `Product ID`, Category, `Sub-Category`, `Product Name`,`Return Product ID`, `Return Order ID`, Manufacturer, `Return Reason`, description_for_return, Approver 
# MAGIC
# MAGIC   FROM Orders_and_Returns_raw
# MAGIC );
# MAGIC
# MAGIC -- Display the new temp view
# MAGIC SELECT * FROM Orders_and_Returns
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Confirm whether sales returned 0 for returned items.
# MAGIC
# MAGIC -- SELECT count(Sales)
# MAGIC -- FROM Orders_and_Returns
# MAGIC -- WHERE Sales = 0;

# COMMAND ----------

#Now that our Orders_and_returns data is ready lets look into the Quota Data
display(dbutils.fs.ls('/FileStore/Project1/'));

#The Quota data is in .xlsx format. The headers of the table includes integer type. So we need to construct a schema before reading the file since the excel library connecting databricks only read headers in string format.

#import the required datatypes.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the Excel file
schema_df = StructType([
    StructField("Region", StringType(), True),
    StructField("2015", IntegerType(), True),
    StructField("2016", IntegerType(), True),
    StructField("2017", IntegerType(), True),
    StructField("2018", IntegerType(), True)
    ])

# Read the XLSX file using Databricks DataFrame API
#The Quota file also contain unnecessary headers and footers other than the table. To remove this use a sheet name and sheet range while importing.

Quota_df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("sheetName", "Quota")\
            .option("dataAddress", "A4:E8") \
            .schema(schema_df) \
            .load("dbfs:/mnt/Project1/Use_Case_ADF/Quota.xlsx")

#store the value in a temp view
Quota_df.createOrReplaceTempView('Quota_temp_view')

#Display the temp view of Quota
display(Quota_df);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summarize Orders_and_returns to Region and Year with aggregate Sales, Profit and Discount to appropriate aggregations.
# MAGIC -- create a CTE with it
# MAGIC -- Unpivot the Quota data and create a CTE with it
# MAGIC -- Join aggregated CTE Data with Quota Data
# MAGIC --save it in a new temp view
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW Quota_to_sales AS(
# MAGIC     WITH Quota AS(
# MAGIC     SELECT *
# MAGIC     FROM Quota_temp_view UNPIVOT INCLUDE NULLS
# MAGIC     (`Sales Quota` FOR Year IN (`2015`,`2016`,`2017`,`2018`))
# MAGIC     ),
# MAGIC     Orders_and_returns_agg AS(
# MAGIC         SELECT  Region, 
# MAGIC                 YEAR(`Order Date`) AS Year,
# MAGIC                 round(SUM(sales),2) AS sales,
# MAGIC                 round(SUM(Profit),2) AS Profit,
# MAGIC                 round(SUM(Discount),2) AS Discount
# MAGIC         FROM Orders_and_Returns
# MAGIC         GROUP BY Region, YEAR(`Order Date`)
# MAGIC     )
# MAGIC
# MAGIC      SELECT q.*,
# MAGIC             o.sales, 
# MAGIC             o.profit, 
# MAGIC             o.Discount,
# MAGIC             -- Create a derived field (Quota_Achievement) that calculates Quota achievement in percentage against total sales. And a KPI field which defines if:
# MAGIC             -- 1.	Quota_Achievement > 100 then “over_achieved”
# MAGIC             -- 2.	Quota_Achievement including 90 and 100 or between then “Just_There”
# MAGIC             -- 3.	Quota_Achievement less than 90 “Not_there”
# MAGIC             round(((o.sales/q.`Sales Quota`)*100),2) AS Quota_Achievement,
# MAGIC             CASE
# MAGIC             WHEN ((o.sales/q.`Sales Quota`)*100) > 100 THEN 'over_achieved'
# MAGIC             WHEN ((o.sales/q.`Sales Quota`)*100) <= 100 AND ((o.sales/q.`Sales Quota`)*100) >= 90 THEN 'Just_There'
# MAGIC             ELSE 'Not_there'
# MAGIC             END AS Sales_KPI
# MAGIC             FROM Quota AS q
# MAGIC             LEFT JOIN Orders_and_returns_agg AS o
# MAGIC             ON q.Region = o.Region AND q.Year = o.Year
# MAGIC             ORDER BY 1,2
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM Quota_to_sales;

# COMMAND ----------

result_df = spark.sql("""
SELECT *
FROM Quota_to_sales
""")
result_df.coalesce(1).write.csv('dbfs:/mnt/Project1/result', header=True, mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`dbfs:/mnt/Project1/result`
