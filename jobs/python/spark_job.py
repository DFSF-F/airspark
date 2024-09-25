import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

end_date = sys.argv[1]
start_date = sys.argv[2]

spark = SparkSession.builder \
    .appName("UserActionAggregation") \
    .getOrCreate()

input_dir = "/opt/bitnami/spark/repos_csv/input"

all_files = os.listdir(input_dir)

selected_files = []
for file_name in all_files:
    if file_name.endswith(".csv"):
        try:
            file_date = file_name.replace(".csv", "")

            if start_date <= file_date <= end_date:
                selected_files.append(os.path.join(input_dir, file_name))
        except ValueError:
            print(f'Название файла {file_name} не соответствует формату %Y-%m-%d')
            continue
    else:
        print(f'Файл {file_name} не является CSV')

if selected_files:
    df = spark.read.csv(selected_files, header=False)
    df = df.toDF("email", "action", "dt")

    df = df.withColumn("dt", col("dt").cast("date"))

    agg_df = df.groupBy("email").pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"]).count()

    agg_df.show()
    agg_df = agg_df.na.fill(0)

#     agg_df.write.csv(f'./repos_csv/output/{end_date}.csv')
else:
     print("No files found for the given date range.")

spark.stop()
