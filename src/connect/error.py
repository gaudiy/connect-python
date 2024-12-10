# Copyright 2024 Gaudiy, Inc.
# SPDX-License-Identifier: Apache-2.0

import httpx

from connect.code import Code


class Error(httpx.HTTPStatusError):
    """
    ConnectError captures four pieces of information: a Code, an error
    message, an optional cause of the error, and an optional collection of
    arbitrary Protobuf messages called  "details".

    Because developer tools typically show just the error message, we prefix
    it with the status code, so that the most important information is always
    visible immediately.

    Error details are wrapped with google.protobuf.Any on the wire, so that
    a server or middleware can attach arbitrary data to an error. Use the
    method findDetails() to retrieve the details.

    Attributes:
        code (code.Code): The error code associated with the exception.
        message (str): The error message associated with the exception.
    """

    def __init__(self, code, message):
        """
        Initializes a new instance of the Error class.

        Args:
            code (int): The error code.
            message (str): The error message.
        """
        try:
            self.code = Code(code)
        except ValueError:
            self.code = Code.UNKNOWN
        self.message = message
