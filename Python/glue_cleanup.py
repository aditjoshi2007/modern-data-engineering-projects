############################### OBJECTIVE ###########################################
# To delete all glue tables either based on the prefix or name given, also to delete the crawlers if given.
# Argument region is mandatory
# Argument database is mandatory only if table deletion is required
# Arguments prefix and tables are not mandatory , but need at least one of them to delete tables (comma separated list or a single value)
# Argument prefix will list the table names with given prefixes and delete the tables
# Argument crawlers is not mandatory, if given it will delete the crawler (comma separated list or a single value)
# Take confirmation and then delete.
################################ INVOKE EXAMPLES #######################################
# python3 glue_cleanup.py --region aws_region --database layer_name --prefix sf --crawlers tables_layer_name
# python3 glue_cleanup.py --region aws_region --database layer_name --tables tbl_name
# python3 glue_cleanup.py --region aws_region --crawlers tables_layer_name
#######################################################################################

import boto3
import argparse
import traceback


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--region', help="Provide region name", required=True)
    parser.add_argument('--database', help="Provide database name", required=False)
    parser.add_argument('--prefix', help="Provide prefix of tables to be deleted", required=False)
    parser.add_argument('--tables', help="Provide tables to be deleted", required=False)
    parser.add_argument('--crawlers', help="Provide crawlers to be deleted", required=False)

    args = parser.parse_args()

    if args.region is None:
        print('\n__main__: --region mandatory argument is not passed, exiting...')
        exit(1)

    glue_client = boto3.client('glue', region_name=args.region)

    if args.database is None:
        print('\n__main__: --database argument is not passed, mandatory for deleting tables, skipping... \n')
    else:
        try:
            response = glue_client.get_database(Name=args.database)
            if response['Database']['Name'] == args.database:
                print(f"\n'{args.database}' is a valid database\n")
        except:
            print(f"\n'{args.database}' is not a valid database\n")
            print(traceback.format_exc())
            exit(1)

        if args.prefix is None:
            print('\n__main__: --prefix is not passed, will be using table names instead. \n')
            if args.tables is None:
                print('\n__main__: --tables, prefix parameter are not passed, need at least one of them to '
                      'delete tables. \n')
            else:
                for table in args.tables.split(','):
                    print(f'will delete table: {table}\n')
                    #confirmation = input('\nEnter "delete" if you want to permanently delete the above table:\n\n')
                    #if confirmation == 'delete':
                    try:
                        res = glue_client.delete_table(DatabaseName=args.database, Name=table)
                    except:
                        print(f'\nFollowing error occurred while trying to delete table: {table}:\n')
                        print(traceback.format_exc())
                        #exit(1)
                    #else:
                        #print('\nNOT DELETED as the input is not exactly "delete" word\n')
        else:
            try:
                paginator = glue_client.get_paginator('get_tables')
                page_iterator = paginator.paginate(DatabaseName=args.database)
                page_details = [page['TableList'] for page in page_iterator]
                final_tables = [item.get('Name') for page in page_details for item in page]

                for prefix in args.prefix.split(','):
                    print(f'working on prefix: {prefix}\n')
                    tables = [item for item in final_tables if item.startswith(prefix+'_')]
                    print(f"No of tables with prefix '{prefix}' : {len(tables)} \n")
                    if tables:
                        print(f'Following tables will be deleted:\n\n {tables}')
                        #confirmation = input('\nEnter "delete" if you want to permanently delete the above list of '
                        #                     'tables:\n\n')
                        #if confirmation == 'delete':
                        try:
                            res_ls = [glue_client.delete_table(DatabaseName=args.database, Name=table)
                                      for table in tables]
                        except:
                            print(f'\nFollowing error occurred while trying to delete tables with '
                                  f'prefix: {prefix}:\n')
                            print(traceback.format_exc())
                            #exit(1)
                        #else:
                        #    print('\nNOT DELETED as the input is not exactly "delete" word\n')
                    else:
                        print(f'No table found with the prefix: {prefix}\n')
            except:
                print('\nFollowing error occurred:\n')
                print(traceback.format_exc())
                exit(1)

    if args.crawlers is None:
        print('\n__main__: --crawlers to be deleted are not passed, skipping...\n')
        exit(1)
    else:
        for crawler in args.crawlers.split(','):
            print(f'working on crawler: {crawler}\n')
            #confirmation = input('\nEnter "delete" if you want to permanently delete the above mentioned crawler:\n\n')
            #if confirmation == 'delete':
            try:
                response = glue_client.delete_crawler(Name=crawler)
            except:
                print(f'\nFollowing error occurred while trying to delete crawler: {crawler}:\n')
                print(traceback.format_exc())
                #exit(1)
            #else:
                #print('\nNOT DELETED as the input is not exactly "delete" word\n')
    print('\n-----EOP-----')