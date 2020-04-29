# Databricks notebook source
from pyspark.sql.functions import *

locazioni = spark.read.format('csv').load('/FileStore/tables/taxi__zone_lookup-b0a97.csv', inferSchema = True, header = True)

g2011 = spark.read.format('csv').load('/FileStore/tables/2011_gialli.csv', inferSchema = True, header = True)
g2012 = spark.read.format('csv').load('/FileStore/tables/2012_gialli.csv', inferSchema = True, header = True)
g2013 = spark.read.format('csv').load('/FileStore/tables/2013_gialli.csv', inferSchema = True, header = True)
g2014 = spark.read.format('csv').load('/FileStore/tables/2014_gialli.csv', inferSchema = True, header = True)
g2015 = spark.read.format('csv').load('/FileStore/tables/2015_gialli.csv', inferSchema = True, header = True)
g2016 = spark.read.format('csv').load('/FileStore/tables/2016_gialli.csv', inferSchema = True, header = True)
g2017 = spark.read.format('csv').load('/FileStore/tables/2017_gialli.csv', inferSchema = True, header = True)
g2018 = spark.read.format('csv').load('/FileStore/tables/2018_gialli.csv', inferSchema = True, header = True)

v2013 = spark.read.format('csv').load('/FileStore/tables/2013_verdi.csv', inferSchema = True, header = True)
v2014 = spark.read.format('csv').load('/FileStore/tables/2014_verdi.csv', inferSchema = True, header = True)
v2015 = spark.read.format('csv').load('/FileStore/tables/2015_verdi.csv', inferSchema = True, header = True)
v2016 = spark.read.format('csv').load('/FileStore/tables/2016_verdi.csv', inferSchema = True, header = True)
v2017 = spark.read.format('csv').load('/FileStore/tables/2017_verdi.csv', inferSchema = True, header = True)
v2018 = spark.read.format('csv').load('/FileStore/tables/2018_verdi.csv', inferSchema = True, header = True)

# COMMAND ----------

v2015 = v2015.drop('improvement_surcharge')
v2016 = v2016.drop('improvement_surcharge')
v2017 = v2017.drop('improvement_surcharge')
v2018 = v2018.drop('improvement_surcharge')
v2013 = v2013.drop('Ehail_fee')
v2014 = v2014.drop('Ehail_fee')

# COMMAND ----------

v2013 = v2013.withColumnRenamed("Store_and_fwd_flag", 'store_and_fwd_flag')
v2013 = v2013.withColumnRenamed("Payment_type", 'payment_type')
v2013 = v2013.withColumnRenamed("Trip_type", 'trip_type')
v2013 = v2013.withColumnRenamed("Extra", 'extra')

v2014 = v2014.withColumnRenamed("Store_and_fwd_flag", 'store_and_fwd_flag')
v2014 = v2014.withColumnRenamed("Payment_type", 'payment_type')
v2014 = v2014.withColumnRenamed("Trip_type", 'trip_type')
v2014 = v2014.withColumnRenamed("Extra", 'extra')

v2015 = v2015.withColumnRenamed("Store_and_fwd_flag", 'store_and_fwd_flag')
v2015 = v2015.withColumnRenamed("Payment_type", 'payment_type')
v2015 = v2015.withColumnRenamed("Trip_type", 'trip_type')
v2015 = v2015.withColumnRenamed("Extra", 'extra')

# COMMAND ----------

v2013 = v2013.withColumn('anno', year('pickup_datetime'))
v2014 = v2014.withColumn('anno', year('pickup_datetime'))
v2015 = v2015.withColumn('anno', year('pickup_datetime'))
v2016 = v2016.withColumn('anno', year('pickup_datetime'))
v2017 = v2017.withColumn('anno', year('pickup_datetime'))
v2018 = v2018.withColumn('anno', year('pickup_datetime'))

# COMMAND ----------

v2013=v2013.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount','payment_type','trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
v2014=v2014.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount','payment_type','trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
v2015=v2015.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount','payment_type','trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
v2016=v2016.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount','payment_type','trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
v2017=v2017.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount','payment_type','trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
v2018=v2018.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount','payment_type','trip_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')

# COMMAND ----------

# Green Union
verde=v2013.union(v2014).union(v2015).union(v2016).union(v2017).union(v2018)

# COMMAND ----------

display(verde.groupBy("VendorID").count())

# COMMAND ----------

g2011 = g2011.withColumn('anno', year('pickup_datetime'))
g2012 = g2012.withColumn('anno', year('pickup_datetime'))
g2013 = g2013.withColumn('anno', year('pickup_datetime'))
g2014 = g2014.withColumn('anno', year('pickup_datetime'))
g2015 = g2015.withColumn('anno', year('pickup_datetime'))
g2016 = g2016.withColumn('anno', year('pickup_datetime'))
g2017 = g2017.withColumn('anno', year('pickup_datetime'))
g2018 = g2018.withColumn('anno', year('pickup_datetime'))

# COMMAND ----------

g2011 = g2011.withColumnRenamed('surcharge', 'extra')
g2012 = g2012.withColumnRenamed('surcharge', 'extra')
g2013 = g2013.withColumnRenamed('surcharge', 'extra')
g2014 = g2014.withColumnRenamed('surcharge', 'extra')

# COMMAND ----------

g2011=g2011.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
g2012=g2012.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
g2013=g2013.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
g2014=g2014.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
g2015=g2015.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
g2016=g2016.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
g2017=g2017.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')
g2018=g2018.select('VendorID','pickup_datetime','dropoff_datetime','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount',"extra",'mta_tax','tip_amount','tolls_amount','total_amount','payment_type','zona_prelievo','zona_scarico','mese','giorno','ora','anno')

# COMMAND ----------

# Yellow Union
giallo=g2011.union(g2012).union(g2013).union(g2014).union(g2015).union(g2016).union(g2017).union(g2018)

# COMMAND ----------

# Errors cleaning
verde=verde.filter(verde["anno"]!=2009)
giallo=giallo.filter(giallo["trip_distance"]<502)

# COMMAND ----------

guadagniverdev=verde.groupBy("VendorID").avg("fare_amount")
guadagniverdev=guadagniverdev.withColumnRenamed("avg(fare_amount)", "Verde")
guadagniverdev=guadagniverdev.orderBy("VendorID")

guadagnigiallov=giallo.groupBy("VendorID").avg("fare_amount")
guadagnigiallov=guadagnigiallov.withColumnRenamed("avg(fare_amount)", "Giallo")
guadagnigiallov=guadagnigiallov.orderBy("VendorID")


guadagniv=guadagnigiallov.join(guadagniverdev,guadagniverdev["VendorID"]==guadagnigiallov["VendorID"], "outer").drop(guadagniverdev["VendorID"])

# COMMAND ----------

display(giallo.groupBy("VendorID").count())
display(verde.groupBy("VendorID").count())

# COMMAND ----------

#3.3.1
guadagniv=guadagniv.orderBy('VendorID')
guadagniv=guadagniv.withColumn("Giallo", round(guadagniv["Giallo"], 2)).withColumn("Verde", round(guadagniv["Verde"], 2))
display(guadagniv)

# COMMAND ----------

# Average green distance
distanzaverdi=verde.groupBy("VendorID","anno").avg("trip_distance")
distanzaverdi=distanzaverdi.withColumnRenamed("avg(trip_distance)", "distance")
distanzaverdi=distanzaverdi.orderBy("anno")

# Average yellow distance
distanzagialli=giallo.groupBy("VendorID","anno").avg("trip_distance")
distanzagialli=distanzagialli.withColumnRenamed("avg(trip_distance)", "distance")
distanzagialli=distanzagialli.orderBy("anno")


# COMMAND ----------

guadagniverde=verde.groupBy("anno").avg("fare_amount")
guadagniverde=guadagniverde.withColumnRenamed("avg(fare_amount)", "Verde")
guadagniverde=guadagniverde.orderBy("anno")

guadagnigiallo=giallo.groupBy("anno").avg("fare_amount")
guadagnigiallo=guadagnigiallo.withColumnRenamed("avg(fare_amount)", "Giallo")
guadagnigiallo=guadagnigiallo.orderBy("anno")

guadagni=guadagnigiallo.join(guadagniverde,guadagniverde["anno"]==guadagnigiallo["anno"], "outer").drop(guadagniverde["anno"])


# COMMAND ----------

distanzeverde=verde.groupBy("anno").avg("trip_distance")
distanzeverde=distanzeverde.withColumnRenamed("avg(trip_distance)", "Verde")
distanzeverde=distanzeverde.orderBy("anno")

distanzegiallo=giallo.groupBy("anno").avg("trip_distance")
distanzegiallo=distanzegiallo.withColumnRenamed("avg(trip_distance)", "Giallo")
distanzegiallo=distanzegiallo.orderBy("anno")

distanze_annuali=distanzegiallo.join(distanzeverde,distanzeverde["anno"]==distanzegiallo["anno"], "outer").drop(distanzeverde["anno"])

# COMMAND ----------

paggiallo=giallo.groupBy("anno","payment_type").count()
paggiallo=paggiallo.orderBy("anno")



pagverde=verde.groupBy("anno","payment_type").count()
pagverde=pagverde.orderBy("anno")

# COMMAND ----------

# Total tips count
tipcountg=g2017.filter(g2017["tip_amount"]>0).count()
tipcountv=v2017.filter(v2017["tip_amount"]>0).count()
print(tipcountg,tipcountv)

# Yellow tips
mancevendorgialli=giallo.groupBy("VendorID").avg("tip_amount")
mancevendorgialli=mancevendorgialli.withColumnRenamed("avg(tip_amount)", "mance_gialli")

# Green Tips
mancevendorverdi=verde.groupBy("VendorID").avg("tip_amount")
mancevendorverdi=mancevendorverdi.withColumnRenamed("avg(tip_amount)", "mance_verdi")

mance_tot=mancevendorgialli.join(mancevendorverdi,"VendorID")
display(mance_tot)

# COMMAND ----------

# November 2016
# Mean 336k

# dataset november
novembre8_2016=spark.read.format('csv').load('/FileStore/tables/novembre8_2016.csv', inferSchema = True, header = True)
novembre13_2016=spark.read.format('csv').load('/FileStore/tables/novembre13.csv', inferSchema = True, header = True)
novembre24_2016=spark.read.format('csv').load('/FileStore/tables/novembre24_2016.csv', inferSchema = True, header = True)
novembre30_2016=spark.read.format('csv').load('/FileStore/tables/novembre30_2016.csv', inferSchema = True, header = True)



# COMMAND ----------

# novembre 8
novembre8_2016=novembre8_2016.groupBy("ora").count()
novembre8_2016=novembre8_2016.orderBy("ora")
novembre8_2016=novembre8_2016.withColumnRenamed("count","8 Novembre")
# novembre 13
novembre13_2016=novembre13_2016.groupBy("ora").count()
novembre13_2016=novembre13_2016.orderBy("ora")
novembre13_2016=novembre13_2016.withColumnRenamed("count","13 Novembre")
# novembre 24
novembre24_2016=novembre24_2016.groupBy("ora").count()
novembre24_2016=novembre24_2016.orderBy("ora")
novembre24_2016=novembre24_2016.withColumnRenamed("count","24 Novembre")
# novembre 30
novembre30_2016=novembre30_2016.groupBy("ora").count()
novembre30_2016=novembre30_2016.orderBy("ora")
novembre30_2016=novembre30_2016.withColumnRenamed("count","30 Novembre")


# COMMAND ----------

uniti=novembre8_2016.join(novembre13_2016, "ora").join(novembre24_2016, "ora").join(novembre30_2016, "ora")
display(uniti)

# COMMAND ----------

# October 2012
# Mean 468k

#dataset ottobre
ottobre29_2012=spark.read.format('csv').load('/FileStore/tables/ottobre29_2012.csv', inferSchema = True, header = True)
ottobre30_2012=spark.read.format('csv').load('/FileStore/tables/ottobre30_2012.csv', inferSchema = True, header = True)
ottobre10_2012=spark.read.format('csv').load('/FileStore/tables/ottobre10_2012.csv', inferSchema = True, header = True)

# COMMAND ----------

# ottobre 10
ottobre10_2012=ottobre10_2012.groupBy("ora").count()
ottobre10_2012=ottobre10_2012.orderBy("ora")
ottobre10_2012=ottobre10_2012.withColumnRenamed("count","10 Ottobre")

# ottobre 29
ottobre29_2012=ottobre29_2012.groupBy("ora").count()
ottobre29_2012=ottobre29_2012.orderBy("ora")
ottobre29_2012=ottobre29_2012.withColumnRenamed("count","29 Ottobre")

# ottobre 30
ottobre30_2012=ottobre30_2012.groupBy("ora").count()
ottobre30_2012=ottobre30_2012.orderBy("ora")
ottobre30_2012=ottobre30_2012.withColumnRenamed("count","30 Ottobre")

# COMMAND ----------

uniti2=ottobre10_2012.join(ottobre29_2012, "ora").join(ottobre30_2012, "ora")
display(uniti2)

# COMMAND ----------

# Analisi aeroporti
zone=g2017.groupBy("PULocationID").count()
JFK=zone.filter(zone["PULocationID"]==132)
EWR=zone.filter(zone["PULocationID"]==1)
LaGuardia=zone.filter(zone["PULocationID"]==138)
insieme=JFK.union(EWR).union(LaGuardia)
tuttopul=insieme.join(locazioni, insieme.PULocationID==locazioni.LocationID)
tuttopul=tuttopul.drop("service_zone","PULocationID")

zonedol=g2017.groupBy("DOLocationID").count()
JFKdol=zonedol.filter(zonedol["DOLocationID"]==132)
EWRdol=zonedol.filter(zonedol["DOLocationID"]==1)
LaGuardiadol=zonedol.filter(zonedol["DOLocationID"]==138)
insiemedol=JFKdol.union(EWRdol).union(LaGuardiadol)
tuttodol=insiemedol.join(locazioni, insiemedol.DOLocationID==locazioni.LocationID)
tuttodol=tuttodol.drop("service_zone","DOLocationID")

# COMMAND ----------

# Airport Prices
prezzopulJFK=g2017.filter(g2017["PULocationID"]==132)
prezzopulJFK=prezzopulJFK.groupBy("PULocationID").avg("fare_amount")
prezzopulJFK=prezzopulJFK.join(locazioni, prezzopulJFK.PULocationID==locazioni.LocationID)

prezzodolJFK=g2017.filter(g2017["DOLocationID"]==132)
prezzodolJFK=prezzodolJFK.groupBy("DOLocationID").avg("fare_amount")

prezzopulEWR=g2017.filter(g2017["PULocationID"]==1)
prezzopulEWR=prezzopulEWR.groupBy("PULocationID").avg("fare_amount")
prezzopulEWR=prezzopulEWR.join(locazioni, prezzopulEWR.PULocationID==locazioni.LocationID)

prezzodolEWR=g2017.filter(g2017["DOLocationID"]==1)
prezzodolEWR=prezzodolEWR.groupBy("DOLocationID").avg("fare_amount")

prezzopulLG=g2017.filter(g2017["PULocationID"]==138)
prezzopulLG=prezzopulLG.groupBy("PULocationID").avg("fare_amount")
prezzopulLG=prezzopulLG.join(locazioni, prezzopulLG.PULocationID==locazioni.LocationID)

prezzodolLG=g2017.filter(g2017["DOLocationID"]==138)
prezzodolLG=prezzodolLG.groupBy("DOLocationID").avg("fare_amount")

prezzimediaeroportipul=prezzopulJFK.union(prezzopulEWR).union(prezzopulLG)
prezzidol=prezzodolJFK.union(prezzodolEWR).union(prezzodolLG)

# COMMAND ----------


