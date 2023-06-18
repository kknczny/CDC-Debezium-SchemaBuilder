from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
from schemabuilder import SchemaBuilder

class DeserializerCDC:
    def __init__(self, df, schema_type):
        builder = SchemaBuilder(schema_type)
        
        self.df = df
        self.cdc_schema=builder.get_cdc_schema()
        self.root_schema=builder.get_full_schema_cdc()
        
    def deserialize_cdc_msg(self):
        df_parsed = self.df.withColumn("parsed", col("body").cast("string"))
        df_payload = df_parsed  \
                        .select(from_json(col("parsed"), self.root_schema)   \
                        .alias("parsed_json")).select("parsed_json.payload")
        return (
            df_payload.withColumn('value', from_json(to_json('payload'), self.cdc_schema))    \
            .select(    \
            'value.op', \
            'value.after.*',    \
            (col('value.ts_ms') / 1000).cast(TimestampType()).alias('ts'))
        )
    
    def deserialize_cdc_msg_json(self):
        df_payload = self.df.select('payload')

        return (
            df_payload.withColumn('value', from_json(to_json('payload'), self.cdc_schema))
                .select(
                'value.op',
                'value.after.*',
                (col('value.ts_ms') / 1000).cast(TimestampType()).alias('ts'))
        )

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.read.option("multiline", "true").json("sample_orders_event.json")
    deserial = DeserializerCDC(df, "orders")
    deserialized_df = deserial.deserialize_cdc_msg_json()
    deserialized_df.show()