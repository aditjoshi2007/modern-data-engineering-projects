
from loaders.full_load_cdc_staging_serial import manifest_fulload_json

def test_manifest_returns_copy_sql(tmp_path):
    sql = manifest_fulload_json(
        manifest_bucket='my-bucket',
        manifest_key='manifests/',
        table_name='orders',
        fullload_files_list=['s3://bucket/file1.json'],
        schema_name='stg',
        role='arn:aws:iam::123:role/redshift',
        file_type='JSON'
    )
    assert 'copy stg.orders' in sql.lower()
    assert 'manifest' in sql.lower()
