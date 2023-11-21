# import sys

# from pyspark.sql import SparkSession

# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: spark-submit app.py <input>")
#         sys.exit(1)

    
#     input_file_path = sys.argv[1]

#     spark = SparkSession.builder.appName("Spark").getOrCreate()
#     df = spark.read.csv(input_file_path)

#     # Show example
#     df.show(20)

#     df.createOrReplaceTempView("df")

    # Queries
    # spark.sql("SELECT * FROM df LIMIT 10").show()
    # spark.sql("SELECT COUNT(*) FROM df").show()
    # spark.sql("SELECT cn, COUNT(*) FROM df GROUP BY cn ORDER BY 2 DESC").show()
    # spark.sql("SELECT cn, MAX(temp) AS max_temp FROM df GROUP BY cn ORDER BY 2 DESC").show()



import sys
import pytest
from pyspark.sql import SparkSession
from test_func import * 


if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("Spark").getOrCreate()
    df = spark.read.csv("test_data.csv")
    assert df.count() > 0, "Dataframe is empty after filtering"

    # def test_filter_column_from_dynamic_frame():
    #     df = filter_column_from_dynamic_frame(df, 'male', 'gender')
    #     assert df.count() > 0, "Dataframe is empty after filtering"


