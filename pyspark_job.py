import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def clean_data(df):
    return (
        df.filter(col("amount") > 0)
        .filter(col("name").isNotNull())
        .withColumn("amount_with_tax", col("amount") * 1.20)
    )


def main(csv_path):
    spark = SparkSession.builder.appName("CustomerOrderCleaning").getOrCreate()
    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_path)
            
            .withColumnRenamed("customer_name", "name")
            .withColumnRenamed("value", "amount")
        )
        clean_data(df).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
