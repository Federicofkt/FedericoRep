# Databricks notebook source
from pyspark.sql.functions import *

# Import datasets
locazioni = spark.read.format('csv').load('/FileStore/tables/taxi__zone_lookup-b0a97.csv', inferSchema = True, header = True)
anno2011=spark.read.format('csv').load('/FileStore/tables/2017_gialli.csv', inferSchema = True, header = True)
"""
# Rinomino colonne
anno2011 = anno2011.withColumnRenamed('pickup_taxizone_id', 'PULocationID')
anno2011 = anno2011.withColumnRenamed('dropoff_taxizone_id', 'DOLocationID')
anno2011 = anno2011.withColumnRenamed('vendor_id', 'VendorID')
anno2011 = anno2011.withColumnRenamed('rate_code', 'RatecodeID')
anno2011 = anno2011.drop("Pickup_longitude","Pickup_latitude","Dropoff_longitude","Dropoff_latitude")
"""
# Green
anno2011 = anno2011.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')
anno2011 = anno2011.withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

anno2011 = anno2011.withColumnRenamed('MTA_tax', 'mta_tax')
anno2011 = anno2011.withColumnRenamed("Passenger_count", 'passenger_count')
anno2011 = anno2011.withColumnRenamed("Trip_distance", 'trip_distance')
anno2011 = anno2011.withColumnRenamed("Fare_amount", 'fare_amount')
anno2011 = anno2011.withColumnRenamed("Tip_amount", 'tip_amount')
anno2011 = anno2011.withColumnRenamed("Tolls_amount", 'tolls_amount')
anno2011 = anno2011.withColumnRenamed("Total_amount", 'total_amount')


# Boroughs
unito = anno2011.join(locazioni, anno2011.PULocationID == locazioni.LocationID)
unito= unito.withColumnRenamed("Borough", "zona_prelievo")
unito=unito.drop("LocationID","Zone","service_zone")
unito2 = unito.join(locazioni, unito.DOLocationID == locazioni.LocationID)
unito2= unito2.withColumnRenamed("Borough", "zona_scarico")
unito2=unito2.drop("Zone","service_zone")
anno2011=unito2.drop("LocationID")

# Aggiungo mese giorno e ora
anno2011=anno2011.withColumn("mese",month(anno2011["pickup_datetime"]))
anno2011=anno2011.withColumn("giorno",dayofmonth(anno2011["pickup_datetime"]))
anno2011=anno2011.withColumn("ora",hour(anno2011["pickup_datetime"]))

# Trasformo vendor e payment (2011-2013)
anno2011=anno2011.withColumn("VendorID", when(anno2011["VendorID"]=="CMT", 1).otherwise(2))
anno2011=anno2011.withColumn("payment_type", when(anno2011["payment_type"]=="CRD", 1).otherwise(2))

# Filtro i payment
anno2011=anno2011.filter(anno2011["payment_type"]!=3)
anno2011=anno2011.filter(anno2011["payment_type"]!=4)
anno2011=anno2011.filter(anno2011["payment_type"]!=5)
