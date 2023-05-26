def extract_csv(spark, files, schemas):
    dfs = []
    for file in files:
        dfs.append(
            spark.read.format("csv").schema(schemas.pop(0)).options(header="True", multiline="True").load(file)
        )
    return dfs