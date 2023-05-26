from decimal import Decimal
from pyspark.sql import SparkSession

from task_one.extracts import extract_csv
from task_one.transforms import transform, cast_result
from task_one.schemas import transformed_schema, accounts_schema, invoice_line_items_schema, invoices_schema, skus_schema
from pyspark.sql.functions import to_date


def test_transform(spark):
    # create sample dataframes for testing
    datasets = ["accounts.csv", "invoice_line_items.csv",
                "invoices.csv", "skus.csv"]
    schemas = [accounts_schema, invoice_line_items_schema, invoices_schema, skus_schema]

    # extract
    dfs = extract_csv(spark, datasets, schemas)

    # execute the transform function
    result = transform(spark, dfs)

    # define expected results
    expected = cast_result(extract_csv(spark, ["results.csv"], [transformed_schema])[0])

    # assert the actual result matches the expected result
    assert result.collect() == expected.collect()
