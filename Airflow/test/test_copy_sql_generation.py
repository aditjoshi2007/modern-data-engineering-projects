
from loaders.full_load_cdc_staging_serial import dms_load_fullload, dms_load_cdc

def test_dms_full_load_sql():
    sql = dms_load_fullload(
        schema_name='stg',
        table_name='users',
        src_schema='id,name',
        manifest_address_file='s3://bucket/users.manifest',
        role='arn:aws:iam::123:role/redshift',
        file_type='JSON'
    )
    assert 'copy stg.users' in sql.lower()
    assert 'manifest' in sql.lower()


def test_dms_cdc_sql():
    sql = dms_load_cdc(
        schema_name='stg',
        table_name='users',
        manifest_address_file='s3://bucket/users_cdc.manifest',
        role='arn:aws:iam::123:role/redshift',
        file_type='JSON',
        raw_current_schema='id,name,op'
    )
    assert 'copy stg.users' in sql.lower()
    assert 'raw_current_schema' not in sql
