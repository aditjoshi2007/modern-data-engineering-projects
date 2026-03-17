from pyspark.sql.types import *
import json
import sys
import traceback
from managers_v6.console_manager import write_log

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
    
            if raw_current_schema == None:
                if (service == 'attunity') and not init_load_flag:
                    header = src_table_schema.split('|') + ["change_seq_col=string", "change_oper_col=string", "change_mask_col=string",
                                                    "stream_position_col=string", "operation_col=string",
                                                    "transaction_id_col=string", "timestamp_col=string", "partition_name_col=string"]
                elif service == 'dms':
                    header = src_table_schema.split('|') + ["change_seq_col=string", "operation_col=string",
                                                    "transaction_id_col=string", "timestamp_col=string"]
                else:
                    header = src_table_schema.split('|')
            else:
                # Need to see if for reading landing data are we using schema, if so when will the raw_current_schema comes as True or False or None 
                header = src_table_schema.split('|') + ["change_seq_col=string", "change_oper_col=string", "change_mask_col=string",
                                                    "stream_position_col=string", "operation_col=string",
                                                    "transaction_id_col=string", "timestamp_col=string",
                                                    "partition_name_col=string", "schema_name_col=string",
                                                    "load_time_col=string", "process_id_col=string"]
                if daily_load:
                    header.append('sha_hash')
    
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
