"""Error classification and retry policies for download tasks."""

from enum import Enum


class ErrorClass(Enum):
    TRANSIENT = "transient"
    AUTH = "auth"
    NOT_FOUND = "not_found"
    DISK = "disk"
    CORRUPTION = "corruption"
    UNKNOWN = "unknown"


RETRY_POLICIES = {
    ErrorClass.TRANSIENT: {"max_retries": 5, "base_delay": 60, "backoff": "exponential"},
    ErrorClass.AUTH: {"max_retries": 0},
    ErrorClass.NOT_FOUND: {"max_retries": 0},
    ErrorClass.DISK: {"max_retries": 3, "base_delay": 300},
    ErrorClass.CORRUPTION: {"max_retries": 2, "base_delay": 30},
    ErrorClass.UNKNOWN: {"max_retries": 2, "base_delay": 120, "backoff": "linear"},
}


class TaskError(Exception):
    def __init__(self, message: str, error_class: ErrorClass = ErrorClass.UNKNOWN):
        super().__init__(message)
        self.classification = error_class.value

    def should_retry(self, current_retry_count: int) -> bool:
        policy = RETRY_POLICIES.get(
            ErrorClass(self.classification), RETRY_POLICIES[ErrorClass.UNKNOWN]
        )
        return current_retry_count < policy["max_retries"]

    def retry_delay(self, current_retry_count: int) -> int:
        policy = RETRY_POLICIES.get(
            ErrorClass(self.classification), RETRY_POLICIES[ErrorClass.UNKNOWN]
        )
        base = policy.get("base_delay", 60)
        backoff = policy.get("backoff", "linear")
        if backoff == "exponential":
            return base * (2 ** current_retry_count)
        return base * (current_retry_count + 1)


def classify_error(exception: Exception, output: str = "") -> ErrorClass:
    """Classify an error from download/upload output."""
    msg = (str(exception) + " " + output).lower()

    if any(k in msg for k in ["401", "403", "gated", "access denied",
                               "token", "unauthorized", "forbidden"]):
        return ErrorClass.AUTH
    if any(k in msg for k in ["404", "not found", "repository not found",
                               "does not exist", "no such"]):
        return ErrorClass.NOT_FOUND
    if any(k in msg for k in ["no space", "disk full", "enospc",
                               "quota exceeded", "errno 28"]):
        return ErrorClass.DISK
    if any(k in msg for k in ["checksum", "corrupt", "integrity",
                               "hash mismatch", "incomplete"]):
        return ErrorClass.CORRUPTION
    if any(k in msg for k in ["timeout", "connection", "reset", "refused",
                               "500", "502", "503", "429", "rate limit",
                               "temporary", "network"]):
        return ErrorClass.TRANSIENT
    return ErrorClass.UNKNOWN
