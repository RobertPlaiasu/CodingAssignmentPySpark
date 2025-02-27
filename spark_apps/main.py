from pyspark.sql import SparkSession


def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Assignment") \
        .getOrCreate()

    data_folder = '/opt/spark/data'
    # read data
    facebook_df = spark.read.csv(data_folder + '/facebook_dataset.csv', header=True, inferSchema=True)

    facebook_df.printSchema()
    google_df = spark.read.csv(data_folder + '/google_dataset.csv', header=True, inferSchema=True)

    google_df.printSchema()

    website_df = spark.read.csv(data_folder + '/website_dataset.csv', header=True, inferSchema=True,
                                sep=';')
    website_df.printSchema()




if __name__ == "__main__":
    main()