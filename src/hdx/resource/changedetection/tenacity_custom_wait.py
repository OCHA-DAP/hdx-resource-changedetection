from http import HTTPStatus
from typing import TYPE_CHECKING, Union

from aiohttp import ClientResponseError
from tenacity import RetryCallState, _utils
from tenacity.wait import wait_base

if TYPE_CHECKING:
    from tenacity import RetryCallState


class custom_wait(wait_base):
    """Wait strategy that applies exponential backoff.

    It allows for a customized multiplier and an ability to restrict the
    upper and lower limits to some maximum and minimum value.

    It also allows min to be multiplied by a factor min_multiplier (defaults to
    8) for certain multiply_codes corresponding to http error codes (defaults
    to 429 too many requests).

    The intervals are fixed (i.e. there is no jitter), so this strategy is
    suitable for balancing retries against latency when a required resource is
    unavailable for an unknown duration, but *not* suitable for resolving
    contention between multiple processes for a shared resource. Use
    wait_random_exponential for the latter case.
    """

    def __init__(
        self,
        multiplier: Union[int, float] = 1,
        max: _utils.time_unit_type = _utils.MAX_WAIT,  # noqa
        exp_base: Union[int, float] = 2,
        min: _utils.time_unit_type = 0,  # noqa
        min_multiplier: int = 8,
        multiply_codes: tuple = (HTTPStatus.TOO_MANY_REQUESTS,),
    ) -> None:
        self.multiplier = multiplier
        self.min = _utils.to_seconds(min)
        self.max = _utils.to_seconds(max)
        self.exp_base = exp_base
        self.min_multiplier = min_multiplier
        self.multiply_codes = multiply_codes

    def __call__(self, retry_state: "RetryCallState") -> float:
        try:
            exp = self.exp_base ** (retry_state.attempt_number - 1)
            result = self.multiplier * exp
        except OverflowError:
            return self.max
        minimum = self.min
        ex = retry_state.outcome.exception()
        # Multiply min wait for certain HTTP error codes
        if isinstance(ex, ClientResponseError) and ex.status in self.multiply_codes:
            minimum *= self.min_multiplier
        return max(max(0, minimum), min(result, self.max))
