
import pytest

from dags.create_emr_dag_polished import poll_cluster_until_ready, poll_steps_until_complete


class FakeEmr:
    """Simple fake EMR client that returns pre-programmed states in FIFO order."""
    def __init__(self, cluster_states, step_states):
        self._cluster_states = list(cluster_states)
        self._step_states = list(step_states)

    def describe_cluster(self, ClusterId):
        state = self._cluster_states.pop(0) if self._cluster_states else 'RUNNING'
        return {'Cluster': {'Status': {'State': state}}}

    def describe_step(self, ClusterId, StepId):
        state = self._step_states.pop(0) if self._step_states else 'COMPLETED'
        return {'Step': {'Status': {'State': state}}}


def test_poll_cluster_until_ready_succeeds():
    emr = FakeEmr(cluster_states=['STARTING', 'BOOTSTRAPPING', 'RUNNING'], step_states=[])
    poll_cluster_until_ready(emr, 'j-123', sleep_seconds=0, max_attempts=5)


def test_poll_cluster_until_ready_fails_on_terminal():
    emr = FakeEmr(cluster_states=['STARTING', 'TERMINATED_WITH_ERRORS'], step_states=[])
    with pytest.raises(RuntimeError):
        poll_cluster_until_ready(emr, 'j-123', sleep_seconds=0, max_attempts=5)


def test_poll_steps_until_complete_succeeds():
    emr = FakeEmr(cluster_states=['RUNNING'], step_states=['PENDING', 'COMPLETED', 'PENDING', 'COMPLETED'])
    poll_steps_until_complete(emr, 'j-123', step_ids=['s-1', 's-2'], sleep_seconds=0, max_attempts=5)


def test_poll_steps_until_complete_fails_on_error():
    emr = FakeEmr(cluster_states=['RUNNING'], step_states=['PENDING', 'FAILED'])
    with pytest.raises(RuntimeError):
        poll_steps_until_complete(emr, 'j-123', step_ids=['s-1', 's-2'], sleep_seconds=0, max_attempts=5)
