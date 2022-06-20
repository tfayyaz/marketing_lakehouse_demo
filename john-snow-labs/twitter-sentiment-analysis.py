# Databricks notebook source
# MAGIC %md
# MAGIC # Twitter Sentiment Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Twitter sample data

# COMMAND ----------

tweets_df = spark.createDataFrame([
  ('boomtown', "@BoomtownFair was incredible today. Loved all the DJs"),
  ('boomtown', "Took ages to get into @BoomtownFair festival. Very badly organised"),
  ('boomtown', "Love all the food at @BoomtownFair!"),
  ('boomtown', "SUPER excited about @boomtown this summer"),
  ('lost_village', "The rain was horrible at @lostvillagefest"),
  ('lost_village', "@lostvillagefest Could not find any good food anywhere :("),
  ('we_out_here', "Really cool to see so many great Jazz artists at @weoutherefest"),
  ('we_out_here', "@weoutherefest had the best time dancing all day and night"),
  ('we_out_here', "@weoutherefest festival life is fun!!!!!"),
  ('we_out_here', "Horrible rain on the first day of @weoutherefest"),
  ('houghton', "Really cold here at @houghtonfstvl. Blame the British weather"),
  ('houghton', "The DJs were so much fun at @houghtonfstvl"),
  ('houghton', "Finally get to go @houghtonfstvl after waiting for so long. Bring on the sunshine and dancing"),
], ["festival_id", "text"])

display(tweets_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Spark NLP lib

# COMMAND ----------

# MAGIC %md
# MAGIC - https://colab.research.google.com/github/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/streamlit_notebooks/SENTIMENT_EN.ipynb#scrollTo=fu9l7NI4N53g

# COMMAND ----------

from pyspark.sql.types import StringType
#Spark NLP
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.annotator import *
from sparknlp.base import *
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Sentiment Analysis pipeline

# COMMAND ----------

MODEL_NAME='sentimentdl_use_twitter'

# COMMAND ----------

documentAssembler = DocumentAssembler()\
    .setInputCol("text")\
    .setOutputCol("document")
    
use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
 .setInputCols(["document"])\
 .setOutputCol("sentence_embeddings")


sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en")\
    .setInputCols(["sentence_embeddings"])\
    .setOutputCol("sentiment")

nlpPipeline = Pipeline(
      stages = [
          documentAssembler,
          use,
          sentimentdl
      ])

# COMMAND ----------

empty_df = spark.createDataFrame([['']]).toDF("text")

pipelineModel = nlpPipeline.fit(empty_df)

# COMMAND ----------

result = pipelineModel.transform(tweets_df)

sentiments_df = (result
                 .select(F.col('festival_id'), F.col('text'), F.explode('sentiment').alias('sentiment'))
                 .select(F.col('festival_id'), F.col('text'), F.col('sentiment.result').alias('sentiment')))

display(sentiments_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data to Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists marketing_twitter;

# COMMAND ----------

sentiments_df.write.format("delta").mode("overwrite").saveAsTable("marketing_twitter.tweets_sentiment")
