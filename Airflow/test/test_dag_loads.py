
import importlib
import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import Variable

def test_dag_import_and_tasks(monkeypatch):
    fake_vars = {
        'environment': 'dev',
        'success_email_html_template': '<p>ok</p>',
        'success_email_subject': 'ok',
        'jobs_list': '{"ibm_db2_source_data":["ibm_db2_source_data_tbls_config"]}',
        'JOB_FLOW_OVERRIDES': '{"ReleaseLabel":"emr-6.10.0","Steps":[],"Instances":{}}',
        'jobs_Ingestion_list': '{"ibm_db2_source_data":"script.py"}',
        'alert_email_list': '["ops@example.com"]',
        'aws_region': 'us-east-1'
    }
    def fake_get(name, default_var=None):
        return fake_vars.get(name, default_var if default_var is not None else "{}")
    monkeypatch.setattr(Variable, 'get', staticmethod(fake_get))

    mod = importlib.import_module('dags.create_emr_dag_polished')
    assert hasattr(mod, 'dag')
    dag = getattr(mod, 'dag')
    tids = [t.task_id for t in dag.tasks]
    for tid in ['create_emr', 'wait_for_cluster_ready', 'wait_for_steps_complete', 'send_success_email']:
        assert tid in tids
