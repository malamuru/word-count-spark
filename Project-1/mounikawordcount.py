
from pyspark.sql import SparkSession

# AWS Credentials
AWS_ACCESS_KEY_ID = 'AKIAWAA66R3XTCBVC66A'
AWS_SECRET_ACCESS_KEY = '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'

# S3 paths
S3_INPUT = 's3a://mounikawordcount/input.txt'
S3_OUTPUT = 's3a://mounikawordcount/output_folder/'

# Spark Session
spark = SparkSession.builder \
    .appName("WordCount") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

# Hadoop S3 Configuration using default AWS credentials
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

# Word Count Logic
text_file = spark.sparkContext.textFile(S3_INPUT)
counts = text_file.flatMap(lambda line: line.split()) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(S3_OUTPUT)

spark.stop()

