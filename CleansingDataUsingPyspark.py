from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import udf, regexp_replace, col, concat_ws, substring, when
from pyspark.sql.types import StringType

spark = (SparkSession
         .builder
         .master('local[*]')
         .appName('Analysis')
         .getOrCreate())


def remove_special_characters(value) -> str:
    return value.replace('/', '').replace('.', '').replace('_', '').strip()


remove_special_characters_udf = udf(remove_special_characters, StringType())


@udf(returnType=StringType())
def remove_special_chars(value):
    return value.replace('/', '').replace('.', '').replace('_', '').strip()


data = pd.read_excel('./output_files/Customer Call List.xlsx')
df = spark.createDataFrame(data)
# renaming column names to proper name
df = df.withColumnsRenamed({'CustomerID': 'Customer_ID', 'Paying Customer': 'Paying_Customer'})
# getting groupBy count
df.groupBy('Customer_ID').count().show()
# dropping duplicate count
df = df.dropDuplicates()
# drop unneccesary columns
df = df.drop('Not_Useful_Column')
# df = df.withColumn('Last_Name', remove_special_characters_udf(df.Last_Name))
df = df.withColumn('Last_Name', remove_special_chars(df.Last_Name))
df = df.withColumn('Phone_Number', regexp_replace(col('Phone_Number'), '[^a-zA-Z0-9]', ''))
df = df.withColumn('Phone_Number', concat_ws('-',
                                             substring(df.Phone_Number, 1, 3),
                                             substring(df.Phone_Number, 4, 3),
                                             substring(df.Phone_Number, 7, 4)))

# Using regexp_replace to change the value #its better to use when and otherwise
df = df.withColumn('Paying_Customer', regexp_replace(df.Paying_Customer, 'Yes', 'Y'))
df = df.withColumn('Paying_Customer', regexp_replace(df.Paying_Customer, 'No', 'N'))

# Using When Otherwise to change the values
df = df.withColumn('Do_Not_Contact',
                   when(df.Do_Not_Contact == 'Yes', 'Y')
                   .when(df.Do_Not_Contact == 'No', 'N')
                   .otherwise(df.Do_Not_Contact))

# df = df.withColumn('Phone_Number', regexp_replace(df.Phone_Number, 'NaN--', ''))
# df = df.withColumn('Phone_Number', regexp_replace(df.Phone_Number, 'Na--', ''))

df = df.withColumn('Phone_Number',
                   when(df.Phone_Number == 'NaN--', 'N/a')
                   .when(df.Phone_Number == 'Na--', 'N/a')
                   .otherwise(df.Phone_Number))

df = df.withColumn('Do_Not_Contact', when(df.Do_Not_Contact == 'NaN', '')
                   .otherwise(df.Do_Not_Contact))

# df.select('*').where('Phone_Number != "N/a" and Do_Not_Contact != "Y" ').show()
df = df.select('*').where(~(df.Phone_Number == 'N/a') & ~(df.Do_Not_Contact == 'Y'))

df.show(truncate=False)

spark.stop()
