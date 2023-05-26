from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, DecimalType

accounts_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("company_address", StringType(), True),
    StructField("contact_person", StringType(), True),
    StructField("contact_phone", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("joining_date", StringType(), True)
])

invoice_line_items_schema = StructType([
    StructField("invoice_id", IntegerType(), True),
    StructField("item_id", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

invoices_schema = StructType([
    StructField("invoice_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("date_issued", StringType(), True),
    StructField("payment_dates", StringType(), True)
])

skus_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("item_description", StringType(), True),
    StructField("item_cost_price", DoubleType(), True),
    StructField("item_retail_price", DoubleType(), True)
])

transformed_schema = StructType([
    StructField("invoice_id", IntegerType(), True),
    StructField("acct_id", IntegerType(), True),
    StructField("inv_total", DecimalType(15,2), True),
    StructField("inv_items", IntegerType(), True),
    StructField("acct_age", IntegerType(), True),
    StructField("number_of_inv120", IntegerType(), True),
    StructField("cum_tot_inv_acct", IntegerType(), True),
    StructField("is_late", IntegerType(), True)
])
