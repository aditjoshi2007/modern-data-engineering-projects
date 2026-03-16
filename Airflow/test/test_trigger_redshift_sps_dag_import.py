
import pytest

airflow = pytest.importorskip("airflow")

def test_dag_import():
    from dags.trigger_redshift_sps_polished import dag
    assert dag is not None
