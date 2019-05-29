// Databricks notebook source
// MAGIC %python
// MAGIC configs = {
// MAGIC 			"fs.azure.account.auth.type": "OAuth",
// MAGIC 			"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC 			"fs.azure.account.oauth2.client.id": "02f67ffc-2e16-43b1-bc74-e819ba0581c2",
// MAGIC 			"fs.azure.account.oauth2.client.secret": "e5S]ZLpE_jOQ/t?scT6ONQKQze0i7OMo",
// MAGIC 			"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token",
// MAGIC 			"fs.azure.createRemoteFileSystemDuringInitialization": "true"
// MAGIC 		}

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.mount(
// MAGIC source = "abfss://data@adbdlsstoragev3.dfs.core.windows.net/",
// MAGIC mount_point = "/mnt/salesdata",
// MAGIC extra_configs = configs)

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/salesdata

// COMMAND ----------

val fileName = "/mnt/salesdata/ratings-c.csv"
val data = sc.textFile(fileName).mapPartitionsWithIndex(
  (index, iterator) => {
    if(index == 0) {
      iterator.drop(1)
    }
    
    iterator
  }).map(line => {
    val splitted = line.split(",")
  
    (splitted(2).toFloat, 1)
  })

val pdata = data.reduceByKey((value1, value2) => value1 + value2).sortBy(_._2).collect

pdata.reverse.foreach(println)

// COMMAND ----------

