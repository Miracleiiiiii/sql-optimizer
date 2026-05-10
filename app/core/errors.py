class SparkAiOptimizerError(Exception):
    """Base exception for domain errors."""


class UpstreamApiError(SparkAiOptimizerError):
    pass


class NotFoundError(SparkAiOptimizerError):
    pass


class LlmError(SparkAiOptimizerError):
    pass

