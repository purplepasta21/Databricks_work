# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1: View the Raw Dataset
# MAGIC
# MAGIC In this step, we display the original loan dataset.
# MAGIC this helps us:
# MAGIC * Understand what the data looks like
# MAGIC * Identify columns and values

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loan

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Inspect the Data Structure
# MAGIC
# MAGIC Here we examine the strucure of the dataset.
# MAGIC
# MAGIC This shows:
# MAGIC * Column names
# MAGIC * Data types
# MAGIC * Overall schema

# COMMAND ----------

# MAGIC %sql
# MAGIC describe loan

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3: Load the dataset into PySpark
# MAGIC
# MAGIC We load the dataset into PySpark DataFrame so we can clean and transform it.
# MAGIC
# MAGIC We create a temporary box "df" and put all of the data into it for manipulation.
# MAGIC

# COMMAND ----------

df = spark.read.table("loan")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 Remove Comment Column 
# MAGIC
# MAGIC The `comment`  column is not needed, so we remove it

# COMMAND ----------

df = spark.read.table("loan")
df_no_comment = df.drop("Comment")
display(df_no_comment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: import Pyspark functions
# MAGIC
# MAGIC We import pyspark functions for data processing and analysis

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 6: Check for Null Values !!IMPORTANT FOR PRACTICAL ASSESSMENT!!
# MAGIC We import 
# MAGIC move dataframes we dont need (comment column) into a separate data frame that wont be used.
# MAGIC
# MAGIC create a copy of a column from the original table so that we can find specific values from it without changing the original table.
# MAGIC

# COMMAND ----------

df_no_comment.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_no_comment.columns
]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Remove Rows with missing loan values
# MAGIC The `loan` Column is essential
# MAGIC Rows with mssing loan values are removed.
# MAGIC
# MAGIC `dropna` is used to removed the null values from a column.

# COMMAND ----------

df = spark.read.table("loan")
df_no_comment = df.drop("Comment")
 
df_clean = df_no_comment.dropna(subset=["Loan"])
 
display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: verify null values have been removed
# MAGIC
# MAGIC Wr need to confimr that no null values remain in the `loan` column.
# MAGIC Loan_nulls may come up with a red underscore, this is expected, and what we want

# COMMAND ----------

import pyspark.sql.functions as F

df_clean.select(
  F.count(F.when(F.col("Loan").isNull(), 1)).alias("Loan_nulls")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step  10: Renaming Columns
# MAGIC
# MAGIC (we skipped nine)  Columns with spacces are renamed using underscores to avoid errors.
# MAGIC The first name put in (eg marital status), is the name of the column you want to change, then the second name (eg Marital_Status) is what it will be renamed to.

# COMMAND ----------

df = spark.read.table("loan")
df_no_comment = df.drop("Comment")
df_clean = df_no_comment.dropna(subset=["Loan"])
 
df_clean = df_clean.withColumnRenamed("Marital Status", "Marital_Status") \
                   .withColumnRenamed("Family Size", "Family_Size") \
                   .withColumnRenamed("Use Frequency", "Use_Frequency") \
                   .withColumnRenamed("Loan Category", "Loan_Category") \
                   .withColumnRenamed("Returned Cheque", "Returned_Cheque") \
                   .withColumnRenamed("Dishonour of Bill", "Dishonour_of_Bill")
 
display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 11: save the cleaned dataset
# MAGIC We save the cleaned dataset as a new, separate table

# COMMAND ----------

df_clean.write.mode("overwrite").saveAsTable("loan_analysis_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: View the cleaned Dataset
# MAGIC
# MAGIC We check that the cleaned table has been saved successfully

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loan_analysis_clean
