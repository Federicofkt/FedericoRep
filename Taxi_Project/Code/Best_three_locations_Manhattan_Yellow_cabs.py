from pyspark.sql.functions import *

locazioni=spark.read.format('csv').load('/FileStore/tables/taxi__zone_lookup-b0a97.csv', inferSchema = True, header = True)

unito = total_gialli_filter.join(locazioni, total_gialli_filter.PULocationID == locazioni.LocationID)
unito = unito.withColumnRenamed("Borough", "zona_prelievo_1")
unito = unito.drop("LocationID","Zone","service_zone")
unito2 = unito.join(locazioni, unito.DOLocationID == locazioni.LocationID)
unito2 = unito2.withColumnRenamed("Borough", "zona_scarico")
unito2 = unito2.drop("Zone","service_zone")

dummy = unito2.filter(unito2["zona_prelievo"]=="Manhattan")
dummy = dummy.groupBy("PULocationID").count()
dummy = dummy.orderBy(dummy["count"], ascending=False).limit(3)
dummy = dummy.join(locazioni, dummy.PULocationID == locazioni.LocationID)
top_3_Mnht_yellow = dummy.select("Count","Borough","Zone").orderBy(dummy["count"], ascending=False)
display(top_3_Mnht_yellow)