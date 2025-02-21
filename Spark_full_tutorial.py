# Databricks notebook source
# MAGIC %md
# MAGIC #Read data

# COMMAND ----------

#path 

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df=spark.read\
      .format('csv')\
      .option("inferschema",True)\
      .option("header",True)\
      .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

#better o/p
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read JSON file 

# COMMAND ----------

df_json=spark.read\
    .format('json')\
        .option("inferschema",True)\
        .option("multiline",False)\
        .option("header",True)\
        .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema definition

# COMMAND ----------

#print schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##DDL schema

# COMMAND ----------

my_ddl_schema='''
        Item_Identifier STRING,
        Item_Weight STRING,
        Item_Fat_Content STRING,
        Item_Visibility DOUBLE,
        Item_Type STRING,
        Item_MRP DOUBLE,
        Outlet_Identifier STRING,
        Outlet_Establishment_Year INT,
        Outlet_Size STRING,
        Outlet_Location_Type STRING,
        Outlet_Type STRING,
        Item_Outlet_Sales DOUBLE

'''

# COMMAND ----------

df=spark.read\
      .format('csv')\
    .schema(my_ddl_schema)\
      .option("header",True)\
      .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## StructType() Schema

# COMMAND ----------

#import libraries

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema=StructType(
    [
        StructField('Item_identifier',StringType(),True),
        StructField('Item_Weight', DoubleType(), True),
        StructField('Item_Fat_Content', StringType(), True),
        StructField('Item_Visibility', StringType(), True),
        StructField('Item_Type', StringType(), True),
        StructField('Item_MRP', DoubleType(), True),
        StructField('Outlet_Identifier', StringType(), True),
        StructField('Outlet_Establishment_Year', IntegerType(), True),
        StructField('Outlet_Size', StringType(), True),
        StructField('Outlet_Location_Type', StringType(), True),
        StructField('Outlet_Type', StringType(), True),
        StructField('Item_Outlet_Sales', DoubleType(), True)
    ]
)

# COMMAND ----------

df=spark.read\
    .format('csv')\
    .option('header',True)\
    .schema(my_struct_schema)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

df_sel=df.select('item_identifier','item_weight','Item_Fat_Content')
df_sel.display()

# COMMAND ----------

#need to import function col
df.select(col('item_identifier'),col('item_type'),col('item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##ALIAS

# COMMAND ----------

df.select(col('item_identifier').alias('item_id'),col('item_type'),col('item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC #### filter based on item_fat_content=Regular

# COMMAND ----------

df.filter(col('item_fat_content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### item type is soft drink and item weight less than 10

# COMMAND ----------

df.filter((col('item_type')=='Soft Drinks') & (col('item_weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outlet location type -> tier 1 or 2     AND outlet size is null

# COMMAND ----------

df.filter(((col('Outlet_Location_Type')=='Tier 1') | (col('Outlet_Location_Type')=='Tier 2')) & (col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### isin() and isNull()

# COMMAND ----------

df.filter((col('Outlet_Location_Type').isin('Tier 1','Tier 2')) & (col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## withColumnRenamed

# COMMAND ----------

df=df.withColumnRenamed('item_weight','item_wt')


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## New column/modify column (withColumn)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###new column

# COMMAND ----------

df=df.withColumn('flag',lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('multiply',col('item_wt')*col('item_mrp'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Replace column values

# COMMAND ----------

df=df.withColumn('Item_Fat_Content',regexp_replace(col('Item_fat_Content'),'Regular','reg'))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','lf'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Typecasting

# COMMAND ----------

df=df.withColumn('Item_wt',col('item_wt').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('Item_wt',col('item_wt').cast(DoubleType()))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort/ orderBy()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sorting by item weight descending

# COMMAND ----------

df.sort(col('item_weight').desc())\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sorting by item_visibility ascending

# COMMAND ----------

df.sort(col('Item_visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sorting by multiple columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### sorting by item_wt descending and item_visibility ascending

# COMMAND ----------

df.sort(['item_wt','item_visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df_x=df.limit(20)
display(df_x)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop

# COMMAND ----------

df_drop=df.drop('Item_Visibility')
df_drop.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###drop multiple columns

# COMMAND ----------

df_drop2=df.drop('Item_visibility','Item_wt')
display(df_drop2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop_duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### entry duplicate

# COMMAND ----------

df.display()
df.dropDuplicates().display()
df.display()

# COMMAND ----------

df_ddc=df.dropDuplicates(subset=['item_type'])
df_ddc.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##UNION and UNION BY NAME

# COMMAND ----------

# MAGIC %md
# MAGIC ####preparing dataframes

# COMMAND ----------

data1=[(1,'Raj'),
       (2,'Ankit')]

schema1='id Integer, name String'

df1=spark.createDataFrame(data1,schema1)


# COMMAND ----------

data2=[(3,'modiji'),
       (4,'rahulji')]

schema2='id Integer, name String'

df2=spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply union

# COMMAND ----------

df1=df1.union(df2)

# COMMAND ----------

df1.display()

# COMMAND ----------

#messed up data

data3=[('Ankit',5),('Raj',6)]
schema='name string, id int'

df3=spark.createDataFrame(data3,schema)
df3.display()

# COMMAND ----------

df1.union(df3).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Union Byname

# COMMAND ----------

df1.unionByName(df3).display()

# COMMAND ----------

data4=[('xyz','abc'),('xyz1','abc1')]
schema4='address string, name string'

df4=spark.createDataFrame(data4,schema4)
df4.display()


# COMMAND ----------

df1.union(df4).display()

# COMMAND ----------

# df1.unionByName(df4).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #String functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##initcap()

# COMMAND ----------

df.select(initcap('Item_Type').alias('capitalize')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##lower()

# COMMAND ----------

df.select(lower('Item_Type').alias('lowercase')).display()

# COMMAND ----------

dfxy=df.withColumn('Item_Type',lower('Item_Type'))
dfxy.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##upper()

# COMMAND ----------

df_up=df.withColumn('Item_Type',upper('Item_Type'))
df_up.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Date functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. current date

# COMMAND ----------

df_date=df.withColumn('Curr_date',current_date())
df_date.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##date_add()

# COMMAND ----------

df_dateAdd=df_date.withColumn('week_after',date_add('curr_date',7))
df_dateAdd.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##date_sub()

# COMMAND ----------

df_dateSub=df_date.withColumn('week_before',date_sub('curr_date',7))
df_dateSub.display()

# COMMAND ----------

df_dateSub1=df_date.withColumn('week_before',date_add('curr_date',-7))
df_dateSub1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## datediff()

# COMMAND ----------

df_dd=df_dateSub.withColumn('date_difference',datediff('curr_date','week_before'))
df_dd.display()

# COMMAND ----------

df_dd=df_dateSub.withColumn('date_difference',
                            datediff('week_before','curr_date'))
df_dd.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Date Format

# COMMAND ----------

df_daF=df_dateSub.withColumn('week_before',date_format('week_before','dd/mm/yy'))
df_daF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dropping Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC #### drop row where all columns are null

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####drop entry where any column null
# MAGIC

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop any when specific column value is null

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replacing Null Values (filling nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC ####replace all null values

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### replace subset col

# COMMAND ----------

df.fillna('NA',subset=['outlet_size']).display()

# COMMAND ----------

df.fillna(0,subset=['multiply']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Split And Indexing

# COMMAND ----------

# MAGIC %md
# MAGIC ####split

# COMMAND ----------

df_sp=df.withColumn('outlet_type',split('outlet_type',' '))
df_sp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####indexing

# COMMAND ----------

df_sp1=df.withColumn('outlet_type',split('outlet_type',' ')[1])
df_sp1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Explode

# COMMAND ----------

#df_sp -> has splitted outlet type column

df_exp=df_sp.withColumn('outlet_type',explode('outlet_type'))
df_exp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## array_contains()

# COMMAND ----------

df_sp.withColumn('Type1_flag',array_contains('outlet_type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Group By

# COMMAND ----------

# MAGIC %md
# MAGIC ####sum of MRP for each item type

# COMMAND ----------

df_sum=df.groupBy('item_type').agg(sum('item_mrp').alias('sum of mrps'))
df_sum.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Average MRP

# COMMAND ----------

df.groupBy('item_type').agg(avg('item_mrp').alias('average MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Group By on multiple columns

# COMMAND ----------

df.groupBy('item_type','outlet_size').agg(sum('item_mrp').alias('total_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple Aggregations in one group By

# COMMAND ----------

df.groupBy('item_type','outlet_size').agg(sum('item_mrp').alias('total_mrp'),avg('item_mrp').alias('average')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Collect_list

# COMMAND ----------

data=[
    ('user1','book1'),
    ('user1','book2'),
    ('user2','book2'),
    ('user2','book3'),
    ('user3','book1'),
    
]

schema='user string, book string'

df_book=spark.createDataFrame(data,schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book').alias('book read')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_size').agg(avg('Item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## When Otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC ####scenario 1: Veg or non-veg?

# COMMAND ----------

df.withColumn('veg_flag',when(col('Item_Type')=="Meat","Non-veg").otherwise('veg'))\
.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### if veg and MRP>100 then veg_expensive if mrp<100 ->veg inexpensive else non-veg

# COMMAND ----------

df.withColumn('label',when(col('Item_type')=='Meat',"Non-veg").otherwise(when(col('item_mrp')>100,"Veg-expensive").otherwise('veg-inexpensive'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###---------------------------------end----------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## INNER JOIN

# COMMAND ----------

#create dataframe

data1=[
    ('1','Gaur','d01'),
    ('2','kit','d02'),
    ('3','Sam','d03'),
    ('4','Tam','d03'),
    ('5','Bam','d05'),
    ('5','Bam','d05'),
    ('6','Ank','d06')

]

schema_d1='emp_id string, emp_name string, dept_id string'

data2=[
    ('d01','HR'),
    ('d02','Marketing'),
    ('d03','Accounting'),
    ('d04','IT'),
    ('d05','Finance'),
]

schema_d2='dept_id string, department string'


df_emp=spark.createDataFrame(data1,schema_d1)
df_dept=spark.createDataFrame(data2,schema_d2)

df_emp.display()
df_dept.display()

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Left Join

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Right Join

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Anti join

# COMMAND ----------

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##--------------------END------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #Window functions

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('row_num',row_number().over(Window.orderBy('Item_identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rank 

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy('Item_identifier'))).display() 

# COMMAND ----------

#desc

df.withColumn('rank',rank().over(Window.orderBy(col('Item_identifier').desc()))).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dense rank

# COMMAND ----------

df.withColumn('dense_rank',dense_rank().over(Window.orderBy('Item_identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cumulative sum

# COMMAND ----------

df.withColumn('cumsum',sum("item_mrp").over(Window.orderBy('item_type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #User defined functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 1 -> create function

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 2: convert into pyspark

# COMMAND ----------

my_udf=udf(my_func)

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3: Use

# COMMAND ----------

df.withColumn("myCol",my_udf('item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # --------------------END----------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #Data writing

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. CSV

# COMMAND ----------

df.write.format('csv')\
    .save('/FileStore/tables/csv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ####append mode

# COMMAND ----------

df.write.format('csv')\
  .mode('append')\
    .save('/FileStore/tables/csv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ####overwrire

# COMMAND ----------

df.write.format('csv')\
  .mode('overwrite')\
    .save('/FileStore/tables/csv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ####error mode

# COMMAND ----------

# df.write.format('csv')\
#   .mode('error')\
#     .save('/FileStore/tables/csv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ignore

# COMMAND ----------

df.write.format('csv')\
  .mode('ignore')\
    .save('/FileStore/tables/csv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parquet

# COMMAND ----------

df.write.format('parquet')\
  .mode('overwrite')\
    .save('/FileStore/tables/csv/data.pq')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Table

# COMMAND ----------

df.write.format('csv')\
  .mode('append')\
    .saveAsTable("ta")

# COMMAND ----------

# MAGIC %md
# MAGIC # --------------------------END-------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ###createTempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from my_view
# MAGIC WHERE Item_Fat_Content='lf'

# COMMAND ----------

# MAGIC %md
# MAGIC #### write it into df

# COMMAND ----------

df_sql=spark.sql("SELECT * from my_view WHERE Item_Fat_Content='lf' ")

# COMMAND ----------

df_sql.display()

# COMMAND ----------


