import argparse
import json

from managers_v6.columns_manager import write_log

class ArgumentsManager:
    # ----------------------------------------------------------------------------------------------------------------------
    # Remove non-ascii characters from columns (cols)
    # ----------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_arguments():
        #***************************************************************
        # Read arguments
        #***************************************************************
        parser = argparse.ArgumentParser()
        parser.add_argument("--run_mode", help="Run Mode (InitialLoad, CDC, Both)")
        parser.add_argument("--table_config", help="Table Config file(Ex: table_name -> config/table_name.json)")
        parser.add_argument("--init_date", help="Initial Load Date (Ex: 20220307)", default="20220307")
        parser.add_argument("--cdc_date", help="CDC Load Date (Ex: 20220309)", default="20220309")
        parser.add_argument("--cdc_prev_date", help="CDC Previous Load Date (Ex: 20220307)", default="20220307")
        args = parser.parse_args()

        #***************************************************************
        # Exit if arguments are incorrect
        #***************************************************************
        if any(v is None for v in [args.run_mode, args.table_config])\
            or args.run_mode not in ["InitialLoad", "CDC", "Both"]:
            print("*******************************************************************")
            print("*** Exiting ***")
            print("Missing: --table_config -> Table Config file(Ex: helper2 -> config/helper2.json)") 
            print("Missing: --run_mode <name (InitialLoad, CDC, Both)>") 
            print("*******************************************************************")
            raise ValueError(f"Missing: --table_config or --run_mode")
           
        return args
    
    @staticmethod
    def get_tableOptions(table_name: str):
        with open(f"config/{table_name}.json") as f:
            return json.load(f)
        
    @staticmethod
    def is_not_list_error(arg, error_msg: str):
        if not isinstance(arg, list):
            raise ValueError(error_msg)
            
               
            
    @staticmethod
    def is_list_empty_or_not_defined(arg, the_default_list : list):
        if not arg:
            write_log(__file__, "is_list_empty_or_not_defined()", 'info', f'List is empty. Defaulting to: {the_default_list}')
            return the_default_list
        else:
            return arg