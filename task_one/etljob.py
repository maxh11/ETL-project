from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from transforms import transform
from extracts import extract_csv
from loads import load_csv
from schemas import *
import sys

def main():
    spark = SparkSession.builder.appName("Reece ETL").getOrCreate()

    datasets = ["data_extracts/accounts.csv", "data_extracts/invoice_line_items.csv",
                "data_extracts/invoices.csv", "data_extracts/skus.csv"]
    schemas = [accounts_schema, invoice_line_items_schema, invoices_schema, skus_schema]


    # extract
    dfs = extract_csv(spark, datasets, schemas)

    # transform
    transformed_df = transform(spark, dfs)

    # load
    load_csv(transformed_df)

    transformed_df.show(20)

    print(f"Data successfully loaded to file system")

if __name__ == '__main__':
    main()
