import logging
import random
import time
from functools import wraps

logger = logging.getLogger(__name__)


class RetryPolicy:
    """Configurable retry policy with exponential backoff and jitter."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 2.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        retryable_exceptions: tuple = (Exception,),
        non_retryable_exceptions: tuple = (RuntimeError, ValueError, KeyError),
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions
        self.non_retryable_exceptions = non_retryable_exceptions

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number (1-based)."""
        delay = self.base_delay * (self.backoff_factor ** (attempt - 1))
        delay = min(delay, self.max_delay)
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        return delay

    def should_retry(self, exception: Exception) -> bool:
        """Determine if an exception is retryable."""
        if isinstance(exception, self.non_retryable_exceptions):
            return False
        return isinstance(exception, self.retryable_exceptions)

    def execute(self, func, *args, **kwargs):
        """Execute a function with retry logic."""
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                if not self.should_retry(e):
                    raise

                if attempt < self.max_retries:
                    delay = self.calculate_delay(attempt)
                    logger.warning(
                        f"Attempt {attempt}/{self.max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries} attempts failed: {e}")

        raise last_exception


def with_retry(
    max_retries: int = 3,
    base_delay: float = 2.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    jitter: bool = True,
):
    """Decorator to add retry logic to a function."""
    policy = RetryPolicy(
        max_retries=max_retries,
        base_delay=base_delay,
        backoff_factor=backoff_factor,
        max_delay=max_delay,
        jitter=jitter,
    )

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return policy.execute(func, *args, **kwargs)
        return wrapper

    return decorator


def create_retry_policy_from_config(config: dict) -> RetryPolicy:
    """Create a RetryPolicy from a config dictionary."""
    retry_config = config.get("retry_policy", {})
    return RetryPolicy(
        max_retries=retry_config.get("max_retries", config.get("max_retries", 3)),
        base_delay=retry_config.get("base_delay", 2.0),
        backoff_factor=retry_config.get("backoff_factor", 2.0),
        max_delay=retry_config.get("max_delay", 60.0),
        jitter=retry_config.get("jitter", True),
    )
