def filter_column_from_dynamic_frame(dyf,filter_value,column_name=str):
    dydf=dyf.filter(col(column_name)==filter_value)
    return dydf

def lowercase_column_values(df,column_name=str):
    df=df.withColumn(column_name,lower(col(column_name)))
    return df