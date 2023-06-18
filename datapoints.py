from  pyspark.sql.types import *

class SchemaMasterClass():
    def get_datapoints(self):
        return self.datapoints

class OrdersSchema(SchemaMasterClass):
    def __init__(self):
        self.datapoints = {
            "OrderID": {
                "type": IntegerType(),
                "nullable": False
            },
            "CustomerID": {
                "type": IntegerType(),
                "nullable": False
            },
            "SalespersonPersonID": {
                "type": IntegerType(),
                "nullable": False
            },
            "PickedByPersonID": {
                "type": IntegerType(),
                "nullable": True
            },
            "ContactPersonID": {
                "type": IntegerType(),
                "nullable": False
            },
            "BackorderOrderID": {
                "type": IntegerType(),
                "nullable": True
            },
            "OrderDate": {
                "type": IntegerType(),
                "nullable": False
            },
            "ExpectedDeliveryDate": {
                "type": IntegerType(),
                "nullable": False
            },
            "CustomerPurchaseOrderNumber": {
                "type": StringType(),
                "nullable": True
            },
            "IsUndersupplyBackordered": {
                "type": BooleanType(),
                "nullable": False
            },
            "Comments": {
                "type": StringType(),
                "nullable": True
            },
            "DeliveryInstructions": {
                "type": StringType(),
                "nullable": True
            },
            "InternalComments": {
                "type": StringType(),
                "nullable": True
            },
            "PickingCompletedWhen": {
                "type": LongType(),
                "nullable": True
            },
            "LastEditedBy": {
                "type": IntegerType(),
                "nullable": False
            },
            "LastEditedWhen": {
                "type": LongType(),
                "nullable": False
            }
        }
