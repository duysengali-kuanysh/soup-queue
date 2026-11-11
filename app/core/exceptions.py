class QueueError(Exception):
    """Base class for expected queue-domain errors."""


class JobNotFoundError(QueueError):
    pass


class InvalidJobStateError(QueueError):
    pass


class LeaseExpiredError(QueueError):
    pass


class StaleWorkerError(QueueError):
    pass


class MaxAttemptsExceededError(QueueError):
    pass


class JobCancelled(QueueError):
    pass


class WorkerStopping(QueueError):
    pass
