import pytest

from src.retry import RetryPolicy, create_retry_policy_from_config, with_retry


class TestRetryPolicy:
    def test_successful_execution(self):
        policy = RetryPolicy(max_retries=3)
        result = policy.execute(lambda: 42)
        assert result == 42

    def test_retries_on_failure(self):
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient failure")
            return "success"

        policy = RetryPolicy(max_retries=3, base_delay=0.01, jitter=False)
        result = policy.execute(flaky)
        assert result == "success"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        policy = RetryPolicy(max_retries=2, base_delay=0.01, jitter=False)

        with pytest.raises(ConnectionError):
            policy.execute(lambda: (_ for _ in ()).throw(ConnectionError("fail")))

    def test_no_retry_on_non_retryable(self):
        call_count = 0

        def bad():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("permanent failure")

        policy = RetryPolicy(max_retries=3, base_delay=0.01)

        with pytest.raises(RuntimeError):
            policy.execute(bad)
        assert call_count == 1

    def test_calculate_delay(self):
        policy = RetryPolicy(base_delay=1.0, backoff_factor=2.0, max_delay=10.0, jitter=False)
        assert policy.calculate_delay(1) == 1.0
        assert policy.calculate_delay(2) == 2.0
        assert policy.calculate_delay(3) == 4.0
        assert policy.calculate_delay(10) == 10.0  # capped at max_delay

    def test_calculate_delay_with_jitter(self):
        policy = RetryPolicy(base_delay=1.0, backoff_factor=2.0, jitter=True)
        delay = policy.calculate_delay(1)
        assert 0.5 <= delay <= 1.0

    def test_should_retry(self):
        policy = RetryPolicy(
            retryable_exceptions=(ConnectionError, TimeoutError),
            non_retryable_exceptions=(RuntimeError,),
        )
        assert policy.should_retry(ConnectionError()) is True
        assert policy.should_retry(RuntimeError()) is False
        assert policy.should_retry(ValueError()) is False


class TestWithRetryDecorator:
    def test_decorator_works(self):
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01, jitter=False)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("transient")
            return "ok"

        assert flaky_func() == "ok"
        assert call_count == 2


class TestCreateRetryPolicyFromConfig:
    def test_default_config(self):
        config = {"max_retries": 5}
        policy = create_retry_policy_from_config(config)
        assert policy.max_retries == 5
        assert policy.base_delay == 2.0

    def test_retry_policy_section(self):
        config = {
            "max_retries": 3,
            "retry_policy": {
                "max_retries": 10,
                "base_delay": 5.0,
                "max_delay": 120.0,
            },
        }
        policy = create_retry_policy_from_config(config)
        assert policy.max_retries == 10
        assert policy.base_delay == 5.0
        assert policy.max_delay == 120.0
