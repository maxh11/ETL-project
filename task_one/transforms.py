from pyspark.sql.functions import sum, count, expr, datediff
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, DecimalType
from pyspark.sql.functions import asc


# leaving these cast functions empty as pyspark can infer the types already but with more complex data and for safety it would be good to set these up
def cast_accounts(df):
    return df


def cast_invoice_line_items(df):
    return df


def cast_invoices(df):
    return df


def cast_skus(df):
    return df


def cast_data(accounts, invoice_line_items, invoices, skus):
    return [cast_accounts(accounts), cast_invoice_line_items(invoice_line_items), cast_invoices(invoices),
            cast_skus(skus)]


def cast_result(df):
    return df.select(
        col("invoice_id").cast(IntegerType()).alias("invoice_id"),
        col("acct_id").cast(IntegerType()).alias("acct_id"),
        col("inv_total").cast(DecimalType(15,2)).alias("inv_total"),
        col("inv_items").cast(IntegerType()).alias("inv_items"),
        col("acct_age").cast(IntegerType()).alias("acct_age"),
        col("number_of_inv120").cast(IntegerType()).alias("number_of_inv120"),
        col("cum_tot_inv_acct").cast(IntegerType()).alias("cum_tot_inv_acct"),
        col("is_late").cast(IntegerType()).alias("is_late")
    ).withColumnRenamed('invoice_id', 'inv_id')


def transform(spark, dfs):
    accounts, invoice_line_items, invoices, skus = cast_data(dfs[0], dfs[1], dfs[2], dfs[3])

    accounts.createOrReplaceTempView('accounts')
    invoice_line_items.createOrReplaceTempView('invoice_line_items')
    invoices.createOrReplaceTempView('invoices')
    skus.createOrReplaceTempView('skus')
    # select data relating to the invoice age and amount
    df = spark.sql("""
        SELECT invoices.invoice_id, ANY_VALUE(accounts.account_id) as acct_id, SUM(skus.item_retail_price * invoice_line_items.quantity) AS inv_total, COUNT(invoice_line_items.item_id) as inv_items, datediff(to_date(ANY_VALUE(invoices.date_issued), "yyyy-MM-dd"), to_date(ANY_VALUE(accounts.joining_date), "yyyy-MM-dd")) AS acct_age
        FROM 
        invoices 
        JOIN accounts on invoices.account_id = accounts.account_id
        JOIN invoice_line_items on invoice_line_items.invoice_id = invoices.invoice_id
        JOIN skus on invoice_line_items.item_id = skus.item_id
        GROUP BY invoices.invoice_id
        ORDER BY acct_id
    """)
    # how many invoices 120 days before this one
    df2 = spark.sql("""
            SELECT i.invoice_id, 
    	    (SELECT COUNT(*) 
            FROM invoices 
            WHERE account_id = i.account_id 
            AND date_issued BETWEEN date_sub(i.date_issued, 120) AND date_sub(i.date_issued, 1)) AS number_of_inv120
            FROM invoices i
    """)
    # how many invoices before this one
    df3 = spark.sql("""
        SELECT i.invoice_id, 
       (SELECT COUNT(*) 
        FROM invoices 
        WHERE account_id = a.account_id 
          AND date_issued < i.date_issued) AS cum_tot_inv_acct
        FROM invoices i
        JOIN accounts a ON a.account_id = i.account_id
    """)
    # how many late invoices
    df4 = spark.sql("""
       SELECT i.invoice_id, 
       CASE WHEN i.payment_dates > date_add(i.date_issued, 30)
            THEN 1
            ELSE 0
       END AS is_late
       FROM invoices i
    """)
    # join all the dataframes on the invoice_id
    joined = df.join(df2, on="invoice_id").join(df3, on="invoice_id").join(df4, on="invoice_id")

    return cast_result(joined).orderBy(asc("inv_id"))
