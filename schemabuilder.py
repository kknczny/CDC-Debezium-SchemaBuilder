from datapoints import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
            print(cls._instances)
        return cls._instances[cls]

class SchemaBuilder(metaclass=SingletonMeta):
    __instance = None

    # @staticmethod
    # def get_instance():
    #     if SchemaBuilder.__instance is None:
    #         SchemaBuilder.__instance = SchemaBuilder()
    #     return SchemaBuilder.__instance

    def __init__(self, schema_name):
        self.schemas = {}
        self.schema_name = schema_name
        
        if schema_name.lower() == "orders":
            schema = OrdersSchema()
        # elif schema_name.lower() == "stock":
        #     schema = StockSchema()
        else:
            raise ValueError(f"Unknown schema: {schema_name}")

        self.schemas[schema_name] = schema
        
    def list_attributes(self):
        schema = self.schemas.get(self.schema_name)
        if schema:
            attributes = schema.get_datapoints()
            print(f"Attributes for {self.schema_name}:")
            for attr, attr_props in attributes.items():
                print(f"datapoint: {attr}")
                for prop, value in attr_props.items():
                    print(f"{prop}: {value}")
                print("-"*15)
        else:
            print(f"Schema {self.schema_name} not found.")
        
    def get_schema_map(self, schema_type="methods"):
        schema = self.schemas.get(self.schema_name)
        if schema:
            attributes = schema.get_datapoints()
        else:
            raise ValueError(f"Schema {self.schema_name} not found.")
            
        if schema_type.lower() == "methods":
            schema_map = StructType()
            for attr, attr_props in attributes.items():
                attr_type = attr_props["type"]
                schema_map = schema_map.add(attr, attr_type)
            return schema_map
        
        if schema_type.lower() == "struct":
            schema_map = []
            for attr, attr_props in attributes.items():
                attr_type = attr_props["type"]
                nullable = attr_props["nullable"]
                struct_field = StructField(attr, attr_type, nullable=nullable)
                schema_map.append(struct_field)
            return schema_map
        else:
            raise ValueError(f"Unknown schema type: {schema_type}")
    
    def get_cdc_schema(self):
        # if obj_name not in OBJ_SCHEMA_MAP.keys():
        #     raise KeyError
        # obj_schema = OBJ_SCHEMA_MAP[obj_name]
        
        obj_schema = self.get_schema_map()
        
        return (
            StructType()
            .add("op", StringType())
            .add("ts_ms", LongType())
            .add("before", obj_schema)
            .add("after", obj_schema)
        )
    
    def get_full_schema_cdc(self, schema_type="struct"):
        
        inner_struct_fields = self.get_schema_map(schema_type)
        
        outer_struct_fields = [
            StructField("before", StructType(inner_struct_fields), nullable=True),
            StructField("after", StructType(inner_struct_fields), nullable=True),
            StructField("source", StructType([
                StructField("version", StringType(), nullable=False),
                StructField("connector", StringType(), nullable=False),
                StructField("name", StringType(), nullable=False),
                StructField("ts_ms", LongType(), nullable=False),
                StructField("snapshot", StringType(), nullable=True),
                StructField("db", StringType(), nullable=False),
                StructField("sequence", StringType(), nullable=True),
                StructField("schema", StringType(), nullable=False),
                StructField("table", StringType(), nullable=False),
                StructField("change_lsn", StringType(), nullable=True),
                StructField("commit_lsn", StringType(), nullable=True),
                StructField("event_serial_no", LongType(), nullable=True)
            ]), nullable=False),
            StructField("op", StringType(), nullable=False),
            StructField("ts_ms", LongType(), nullable=True),
            StructField("transaction", StructType([
                StructField("id", StringType(), nullable=False),
                StructField("total_order", LongType(), nullable=False),
                StructField("data_collection_order", LongType(), nullable=False)
            ]), nullable=True)
        ]

        root_schema = StructType([
            StructField("schema", StructType([
                StructField("type", StringType(), nullable=False),
                StructField("fields", StructType(outer_struct_fields), nullable=False)
            ]), nullable=False),
            StructField("payload", StructType(outer_struct_fields), nullable=True)
        ])
        
        return root_schema
    
if __name__ == "__main__":
    builder = SchemaBuilder("Orders")
    print(builder.get_full_schema_cdc("methods"))
    