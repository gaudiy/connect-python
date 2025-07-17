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

"""Single-use asynchronous response writer for server communication with thread-safe queue mechanism."""

import asyncio

from connect.response import Response


class ServerResponseWriter:
    """A single-use asynchronous response writer for server communication.

    This class provides a thread-safe mechanism for writing and receiving responses
    using an internal asyncio queue with a maximum size of 1. The writer is designed
    for single-use scenarios where only one response can be written and received
    before the writer becomes closed.

    Attributes:
        queue (asyncio.Queue[Response]): Internal queue for storing responses with maxsize=1.
        is_closed (bool): Flag indicating whether the writer has been closed.

    Note:
        The response writer automatically closes after receiving a response,
        making it unsuitable for multiple read/write operations.
    """

    queue: asyncio.Queue[Response]
    is_closed: bool = False

    def __init__(self) -> None:
        """Initialize the ResponseWriter with an async queue.

        Creates an asyncio Queue with a maximum size of 1 to handle response writing
        in an asynchronous manner. The queue acts as a buffer for managing responses
        that need to be written.
        """
        self.queue = asyncio.Queue(maxsize=1)

    async def write(self, response: Response) -> None:
        """Write a response to the queue for processing.

        This method adds a response to the internal queue for asynchronous processing.
        The response writer must not be closed when calling this method.

        Args:
            response (Response): The response object to be written to the queue.

        Raises:
            RuntimeError: If the response writer has been closed and cannot accept
                         new responses.

        Returns:
            None: This method does not return a value.
        """
        if self.is_closed:
            raise RuntimeError("Cannot write to a closed response writer.")

        await self.queue.put(response)

    async def receive(self) -> Response:
        """Asynchronously receive a response from the response writer's queue.

        This method retrieves the next response from the internal queue and marks
        the response writer as closed after receiving the response.

        Returns:
            Response: The response object retrieved from the queue.

        Raises:
            RuntimeError: If the response writer is already closed when attempting
                         to receive a response.

        Note:
            This method can only be called once per response writer instance.
            After calling this method, the response writer will be marked as closed
            and subsequent calls will raise a RuntimeError.
        """
        if self.is_closed:
            raise RuntimeError("Cannot receive from a closed response writer.")

        response = await self.queue.get()
        self.queue.task_done()

        self.is_closed = True
        return response
