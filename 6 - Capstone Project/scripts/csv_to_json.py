import configparser
import os
import re
import json
import boto3
import csv
from itertools import chain
from pyspark import SparkConf
from pyspark.sql import SparkSession

class SaveToJson:
    """ Generates the json data files """

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('/home/workspace/aws.cfg')
        self.aws_id = config['AWS']['AWS_ACCESS_KEY_ID']
        self.aws_secret_key = config['AWS']['AWS_SECRET_ACCESS_KEY']
        self.aws_default_region = config['AWS']['DEFAULT_REGION']
        self.input_data = config['DATA']['INPUT_DATA_PATH']
        self.output_data = config['DATA']['OUTPUT_DATA_PATH']
        self.bucket_name = config['DATA']['BUCKET_NAME']
        self.local_output = config['DATA']['LOCAL_OUTPUT']
        self.local_output_data_path = config['DATA']['LOCAL_OUTPUT_DATA_PATH']
    
    def create_spark_session(self):
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        print('Spark Session created successfully...')
        return spark
        
    def create_bucket(self, bucket_name):
        s3_client = boto3.client(
            's3',
            region_name=self.aws_default_region,
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_secret_key
        )
        print('s3 resource has been set!')

        buckets = s3_client.list_buckets()

        for bucket in buckets['Buckets']:
            if bucket_name == bucket['Name']:
                s3_client.delete_bucket(Bucket=bucket_name)

        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': self.aws_default_region})
        print('s3 bucket has been successfully created!')
        
            
    def process_city_temperature_data(self, spark, input_data, output_data):
        # Read and load the temperature data from csv
        # to a spark dataframe
        temperature_df = spark.read.load(input_data + 'GlobalLandTemperaturesByCity.csv', format="csv", header="true")
        print('Data has been loaded to the dataframe!')

        temperature_df = temperature_df.filter(temperature_df.AverageTemperature != "")
        print('Remove null temperature data from dataframe successfully!')

        # extract columns for the temperature table
        temperature_table = temperature_df.select('dt','AverageTemperature','City','Country','Latitude','Longitude').dropDuplicates()
        temperature_table.write.json(os.path.join(output_data,'city_temperature/'),'overwrite')
        print('City temperature table was successfully written to json.')
        
    def process_immigration_data(self, spark, input_data, output_data):
         # Read and load the immigration data from csv
        # to a spark dataframe
        immig_df = spark.read.load(input_data + 'immigration_data_sample_new.csv', format="csv", header="true")
        print('Immigration Data has been loaded to the dataframe!')

        # extract columns for the temperature table
        immig_table = immig_df.select("i94yr","i94mon","i94cit","i94port","arrdate","i94mode","depdate","i94visa","").dropDuplicates()
        immig_table.write.json(os.path.join(output_data,'immigration/'),'overwrite')
        print('Immigration table was successfully written to json.')
        
    def process_cities_demographics_data(self, spark, input_data, output_data):
        # Read and load the demographics data from csv
        # to a spark dataframe
        
        demographics_df = spark.read.load(input_data + 'us-cities-demographics.csv',sep=';', format="csv", header="true")
        print('Data has been loaded to the dataframe!')

        # extract columns for the demographics table
        demographics_table = demographics_df.select('City','State','Male Population','Female Population','Total Population','State Code')\
                            .withColumnRenamed('City','city') \
                            .withColumnRenamed('State', 'state') \
                            .withColumnRenamed('Male Population', 'male_population') \
                            .withColumnRenamed('Female Population', 'female_population') \
                            .withColumnRenamed('Total Population', 'total_population') \
                            .withColumnRenamed('State Code', 'state_code') \
                            .dropDuplicates()
        demographics_table.write.json(os.path.join(output_data,'city_demographics/'),'overwrite')
        print('City demographics table was successfully written to json.')
        
    def process_airport_code_data(self, spark, input_data, output_data):
         # Read and load the airportration data from csv
        # to a spark dataframe
        airport_df = spark.read.load(input_data + 'airport-codes_csv.csv', format="csv", header="true")
        print('airport Data has been loaded to the dataframe!')

        # extract columns for the demographics table 
        airport_table = airport_df.select("ident","name","continent","iso_country","iso_region","municipality") \
                            .withColumnRenamed('ident','code') \
                            .withColumnRenamed('iso_country','country') \
                            .withColumn('state',split('iso_region', '-')[1]) \
                            .dropDuplicates()
        airport_table.write.json(os.path.join(output_data,'airport_code/'),'overwrite')
        print('airport table was successfully written to json.')
        
    def get_city_key(self, city):
        with open(self.input_data + 'city_code.json') as f:
            city_df = json.load(f)
        for key,value in city_df.items():
            if key == city_df:
                return value
        
    def convert_column(self):
        with open(self.input_data + 'city_code.json') as f:
            data = json.load(f)
        delim = "," # set your own delimiter
        source1 = csv.reader(open(self.input_data + 'immigration_data_sample.csv', "r"), delimiter=delim)
        with open(self.input_data + 'immigration_data_sample_new.csv', "w") as fout:
            csvwriter = csv.writer(fout, delimiter=delim)
            for row in source1:
                if row[6] in data:
                    row[6] = data[row[6]]
                csvwriter.writerow(row)

    def load_aws_cred(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "AKIARYY7NW3W7LEH6THI"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "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"
        os.system("load_aws_cred............")
        
        
    def execute(self):
        self.load_aws_cred()
        
        if self.local_output == "true":
            self.output_data = self.local_output_data_path
        else:
            self.create_bucket(self.bucket_name)

        spark = self.create_spark_session()
        
        method_args = (spark, self.input_data, self.output_data)     
        self.convert_column()
        self.process_city_temperature_data(*method_args)
        self.process_immigration_data(*method_args)


save_to_parquet = SaveToJson()
save_to_parquet.execute()