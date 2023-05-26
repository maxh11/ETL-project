def load_csv(df):
    df.write.format("CSV").mode("overwrite").options(header=True).save("output/result.csv")
    return

# this would be eventually updated to load the data from the data lake into the data warehouse using AWS Glue

