import pytest
from types import SimpleNamespace

from app.core.exceptions import InvalidJobStateError
from app.services.state_machine import transition


def test_allowed_lifecycle_transitions():
    job = SimpleNamespace(status="queued")
    transition(job, "running")
    transition(job, "queued")
    transition(job, "running")
    transition(job, "succeeded")
    assert job.status == "succeeded"


def test_terminal_state_cannot_be_reopened():
    job = SimpleNamespace(status="succeeded")
    with pytest.raises(InvalidJobStateError):
        transition(job, "queued")


def test_queued_can_be_cancelled():
    job = SimpleNamespace(status="queued")
    transition(job, "cancelled")
    assert job.status == "cancelled"
