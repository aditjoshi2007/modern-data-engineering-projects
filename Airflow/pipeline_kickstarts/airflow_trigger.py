import argparse
import base64
import json
import re
import sys
import time

import boto3
import psycopg2
import requests

from get_secrets import get_secret


def list_dag_status(dag_name, mwaa_env_name):
    mwaa_cli_command='dags list-runs'

    client=boto3.client('mwaa', '<aws_region>')
    mwaa_cli_token=client.create_cli_token(Name=mwaa_env_name)
    mwaa_auth_token='Bearer ' + mwaa_cli_token['CliToken']
    mwaa_webserver_hostname='https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])

    raw_data='{0} --dag-id {1} --output {2} --state {3}'.format(mwaa_cli_command, dag_name, 'json', 'running')

    mwaa_response=requests.post(mwaa_webserver_hostname,
                                headers={'Authorization': mwaa_auth_token, 'Content-Type': 'text/json'}, data=raw_data)

    mwaa_std_err_message=base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
    print(f"MWAA call response to list dag runs command : {mwaa_std_err_message}")

    mwaa_std_out_message=base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')
    list_running_dags=json.loads(mwaa_std_out_message)

    return list_running_dags


def trigger_dag(dag_name, mwaa_env_name):
    mwaa_cli_command='dags trigger'
    client=boto3.client('mwaa', '<aws_region>')
    mwaa_cli_token=client.create_cli_token(Name=mwaa_env_name)

    mwaa_auth_token='Bearer ' + mwaa_cli_token['CliToken']
    mwaa_webserver_hostname='https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])
    raw_data='{0} {1}'.format(mwaa_cli_command, dag_name)

    mwaa_response=requests.post(mwaa_webserver_hostname,
                                headers={'Authorization': mwaa_auth_token, 'Content-Type': 'text/plain'}, data=raw_data)

    mwaa_std_err_message=base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
    mwaa_std_out_message=base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')

    exec_date_part_one=re.search('\d{4}-\d{2}-\d{2}T\d{2}', mwaa_std_out_message[:])
    exec_date_part_two=re.search(':\d{2}:\d{2}\+\d{2}:\d{2}', mwaa_std_out_message[:])

    execution_date=exec_date_part_one.group(0) + exec_date_part_two.group(0)

    return mwaa_response.status_code, mwaa_std_err_message, mwaa_std_out_message, execution_date


def get_dag_status(dag_name, execution_date, mwaa_env_name):
    mwaa_cli_command='dags state'
    client=boto3.client('mwaa', '<aws_region>')
    mwaa_cli_token=client.create_cli_token(Name=mwaa_env_name)

    mwaa_auth_token='Bearer ' + mwaa_cli_token['CliToken']
    mwaa_webserver_hostname='https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])
    raw_data='{0} {1} {2}'.format(mwaa_cli_command, dag_name, execution_date)
    mwaa_response=requests.post(mwaa_webserver_hostname,
                                headers={'Authorization': mwaa_auth_token, 'Content-Type': 'text/plain'}, data=raw_data)

    mwaa_std_err_message=base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
    mwaa_std_out_message=base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')

    return mwaa_std_out_message


def get_batch_date(hostname, username, secretname, database_name, port):
    pwd=get_secret(secretname)
    conn=psycopg2.connect(database=database_name, user=username, password=pwd, host=hostname, port=port)
    select_stmt="select batch_date from batch_date_tbl"
    cur=conn.cursor()
    cur.execute(select_stmt)
    fetch_result=[doc for doc in cur]
    batch_date=fetch_result[0][0].strftime('%Y-%m-%d')
    conn.close()
    return batch_date


def get_redshift_audit_status(audit_status, hostname, username, secretname, database_name, port, batch_date, sp_name):
    pwd=get_secret(secretname)
    conn=psycopg2.connect(database=database_name, user=username, password=pwd, host=hostname, port=port)

    while audit_status == False:
        status=[]
        cur=conn.cursor()

        sp_stmt=f"CALL audit_sp('batch_dt','{sp_name}','records');".replace('batch_dt', batch_date)

        cur.execute(sp_stmt)
        my_cursor=conn.cursor("records")
        result=my_cursor.fetchall()
        cur.execute("commit;")
        details_dicts=[doc for doc in result]

        for i in details_dicts:
            status.append(i[5])

        if all(x != "NOTSTARTED" for x in status) and "FAILED" in status:
            msg=f"***** Airflow DAG for {sp_name} failed"
            print(msg)
            exit(1)
        elif all(x == "COMPLETED" for x in status):
            audit_status=True
        else:
            time.sleep(300)
        conn.close()

    return audit_status


def main(env, sp_name, dag_name, mwaa_env_name):
    list_running_dags=list_dag_status(dag_name, mwaa_env_name)
    if len(list_running_dags) == 0:
        audit_status=False
        dag_status=False
        dag_counter=False

        with open(f"redshift_conn_details.json", "r") as f:
            data=json.loads(f.read())

        hostname=data[env]["host"]
        username=data[env]["user"]
        secretname=data[env]["secret_name"]
        database_name=data[env]["database"]
        port=data[env]["port"]

        status_code, err_message, out_message, execution_date=trigger_dag(dag_name, mwaa_env_name)
        print(f"***** {status_code}")
        print(f"***** {err_message}")
        print(f"***** {out_message}")

        while dag_counter == False:
            dag_state=get_dag_status(dag_name, execution_date, mwaa_env_name)
            if 'running' in dag_state:
                time.sleep(600)
            elif 'failed' in dag_state:
                msg=f"***** Airflow DAG status is: failed"
                print(msg)
                exit(1)
                dag_counter=True
            elif 'success' in dag_state:
                msg=f"***** Airflow DAG status is: succeeded"
                print(msg)
                dag_status=True
                dag_counter=True

        # batch_date = get_batch_date(hostname, username, secretname, database_name, port)

        #if dag_name == "dag_name":
            #None
        # else:
        # redshift_status = get_redshift_audit_status(audit_status, hostname, username, secretname, database_name, port, batch_date, sp_name)

        # redshift_status == True
        if dag_status == True:
            msg=f"***** Airflow DAG {dag_name} ran succesfully"
            print(msg)
    else:
        print(f"***** {dag_name} is running currently. Waiting for it's completion")
        dag_state=False
        while dag_state == False:
            time.sleep(600)
            x=list_dag_status(dag_name, mwaa_env_name);
            if len(x) == 0:
                dag_state=True
                print(f"***** DAG {dag_name} has finished it's execution. Discontinuing this run")
                sys.exit()
            print(f"***** {dag_name} is still running")


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--env_name', help="Use dag name from MWAA UI. Required Field", required=True)
    parser.add_argument('--sp_name', help="Use SP name from Redshift. Required Field", required=True)
    parser.add_argument('--dag_name', help="Use dag name from MWAA UI. Required Field", required=True)
    parser.add_argument('--mwaa_env_name', help="Use mwaa env name from MWAA UI. Required Field", required=True)

    args=parser.parse_args()

    if args.env_name is None:
        print("env_name is required.")
        raise MissingArgError
    else:
        env=args.env_name

    if args.sp_name is None:
        print("Use SP name from Redshift.")
        raise MissingArgError
    else:
        sp_name=args.sp_name

    if args.dag_name is None:
        print("dag_name is required. Use dag name from MWAA UI")
        raise MissingArgError
    else:
        dag_name=args.dag_name

    if args.mwaa_env_name is None:
        print("mwaa_env_name is required. Use mwaa env name from MWAA UI. Required Field")
        raise MissingArgError
    else:
        mwaa_env_name=args.mwaa_env_name
    main(env, sp_name, dag_name, mwaa_env_name)
