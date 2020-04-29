# Databricks notebook source
from pyspark.sql.functions import *

gialli_2011 = spark.read.format('csv').load('/FileStore/tables/2011_gialli.csv', inferSchema = True, header=True)
gialli_2012 = spark.read.format('csv').load('/FileStore/tables/2012_gialli.csv', inferSchema = True, header=True)
gialli_2013 = spark.read.format('csv').load('/FileStore/tables/2013_gialli.csv', inferSchema = True, header=True)
gialli_2014 = spark.read.format('csv').load('/FileStore/tables/2014_gialli.csv', inferSchema = True, header=True)
gialli_2015 = spark.read.format('csv').load('/FileStore/tables/2015_gialli.csv', inferSchema = True, header=True)
gialli_2016 = spark.read.format('csv').load('/FileStore/tables/2016_gialli.csv', inferSchema = True, header=True)
gialli_2017 = spark.read.format('csv').load('/FileStore/tables/2017_gialli.csv', inferSchema = True, header=True)
gialli_2018 = spark.read.format('csv').load('/FileStore/tables/2018_gialli.csv', inferSchema = True, header=True)

gialli_2011 = gialli_2011.withColumnRenamed('surcharge', 'extra')
gialli_2012 = gialli_2012.withColumnRenamed('surcharge', 'extra')
gialli_2013= gialli_2013.withColumnRenamed('surcharge', 'extra')
gialli_2014 = gialli_2014.withColumnRenamed('surcharge', 'extra')

gialli_2015 = gialli_2015.drop(gialli_2015['improvement_surcharge'])
gialli_2016 = gialli_2016.drop(gialli_2016['improvement_surcharge'])
gialli_2017 = gialli_2017.drop(gialli_2017['improvement_surcharge'])
gialli_2018 = gialli_2018.drop(gialli_2018['improvement_surcharge'])

#aggiungo la colonna anno

gialli_2011 = gialli_2011.withColumn('anno', year('pickup_datetime'))
gialli_2012 = gialli_2012.withColumn('anno', year('pickup_datetime'))
gialli_2013 = gialli_2013.withColumn('anno', year('pickup_datetime'))
gialli_2014 = gialli_2014.withColumn('anno', year('pickup_datetime'))
gialli_2015 = gialli_2015.withColumn('anno', year('pickup_datetime'))
gialli_2016 = gialli_2016.withColumn('anno', year('pickup_datetime'))
gialli_2017 = gialli_2017.withColumn('anno', year('pickup_datetime'))
gialli_2018 = gialli_2018.withColumn('anno', year('pickup_datetime'))

gialli_2011 = gialli_2011.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')
gialli_2012 = gialli_2012.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')
gialli_2013 = gialli_2013.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')
gialli_2014 = gialli_2014.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')
gialli_2015 = gialli_2015.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')
gialli_2016 = gialli_2016.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')
gialli_2017 = gialli_2017.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')
gialli_2018 = gialli_2018.select('VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'zona_prelievo', 'zona_scarico', 'mese', 'giorno', 'ora', 'anno')

total_gialli = gialli_2011.union(gialli_2012).union(gialli_2013).union(gialli_2014).union(gialli_2015).union(gialli_2016).union(gialli_2017).union(gialli_2018)

# COMMAND ----------

media_fare_mesi = total_gialli.groupBy('mese','anno').avg('fare_amount')
media_fare_mesi = media_fare_mesi.orderBy('anno','mese')
display(media_fare_mesi)

# COMMAND ----------

media_distanza_mesi = total_gialli.groupBy('mese','anno').avg('trip_distance')
media_distanza_mesi = media_distanza_mesi.orderBy('anno','mese')
display(media_distanza_mesi)

# COMMAND ----------


