class SigbotError(Exception):
    """Only allowlisted internal codes may appear in public error reports."""

    exit_code = 3
    execution_attempts: dict | None = None

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class ContractNotVerified(SigbotError):
    exit_code = 2


class AuthorizationRequired(SigbotError):
    exit_code = 5


class SchemaMismatch(SigbotError):
    exit_code = 2


class Conflict(SigbotError):
    pass


class RiskRejected(SigbotError):
    pass


class ReconciliationMismatch(SigbotError):
    exit_code = 4


class TransportUnknown(SigbotError):
    exit_code = 6


class HistoryReadError(TransportUnknown):
    """Allowlisted history error; the reader still validates its official schema."""

    def __init__(self, code, payload):
        super().__init__(code)
        self.payload = payload


class SubmissionNotSent(SigbotError):
    """The transport proved a local failure before the HTTP request began."""


class OrderRejected(SigbotError):
    """A schema-validated official placement rejection, with no accepted order."""


class RateLimited(SigbotError):
    exit_code = 6


class StorageUnavailable(SigbotError):
    exit_code = 6
