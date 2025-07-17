# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Manages the context for a handler, particularly for handling timeouts."""

import time


class HandlerContext:
    """Manages the context for a handler, particularly for handling timeouts.

    This class allows setting a deadline upon initialization and provides a method
    to check the remaining time until that deadline.

    Attributes:
        _deadline (float | None): The timestamp for the deadline, or None if no timeout is set.
    """

    _deadline: float | None

    def __init__(self, timeout: float | None) -> None:
        """Initializes a new handler context.

        Args:
            timeout: The timeout in seconds. If None, no deadline is set.
        """
        self._deadline = time.time() + timeout if timeout else None

    def timeout_remaining(self) -> float | None:
        """Calculates the remaining time in seconds until the handler's deadline.

        If the request has no deadline, this method returns None. Otherwise, it
        returns the difference between the deadline and the current time.

        Returns:
            float | None: The remaining time in seconds, or None if no deadline is set.
        """
        if self._deadline is None:
            return None

        return self._deadline - time.time()
