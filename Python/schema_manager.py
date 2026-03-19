from pyspark.sql.types import *
import json
import sys
import traceback
from console import write_log

class SchemaManager: 
    
    def get_spark_schema(self, src_table_schema, raw_current_schema = None, service = None, init_load_flag=False, daily_load =False):
        try:
            data_type_spark_conversion = {'byte': 'ByteType', 'bit': 'BooleanType', 'short': 'ShortType',
                                          'smallint': 'ShortType', 'tinyint': 'IntegerType', 'int': 'IntegerType',
                                          'integer': 'IntegerType', 'bigint': 'LongType', 'long': 'LongType',
                                          'float': 'DoubleType', 'double': 'DoubleType', 'decimal': 'DecimalType',
                                          'character': 'StringType', 'char': 'StringType', 'string': 'StringType',
                                          'varchar': 'StringType', 'binary': 'BinaryType', 'boolean': 'BooleanType',
                                          'date': 'DateType', 'timestamp': 'TimestampType', 'timestmp': 'TimestampType',
                                          'time': 'StringType', 'datetime': 'TimestampType', 'xml': 'StringType',
                                          'clob': 'StringType', 'blob': 'StringType', 'real': 'FloatType'}
    
            header = src_table_schema
            struct_fields = ''
            for column in header:
                col_name = column.split('=')[0]
                col_datatype = column.split('=')[1]
                if '>' in col_datatype:
                    col_datatype = col_datatype.split('>')[1]
                
                if '(' in col_datatype:
                    pre = col_datatype.split('(')[1][:-1]
                else:
                    pre = ''
                struct_fields = struct_fields + 'StructField("' + col_name.lower() + '",' + data_type_spark_conversion.get(
                    col_datatype.split('(')[0].lower()) + '(' + pre + '),True),'
    
            schema = 'StructType([' + struct_fields[:-1] + '])'
            
            return (eval(schema))
        except Exception as e :
            write_log(__file__, "get_spark_schema()", 'error', f'Error while converting data type \:\n{traceback.format_exc()}')
            exit(1)
