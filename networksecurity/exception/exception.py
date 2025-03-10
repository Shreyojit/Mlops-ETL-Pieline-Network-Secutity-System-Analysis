import sys
from networksecurity.logging import logger

class NetworkSecurityException(Exception):
    def __init__(self, error_message, error_details: sys = None):
        """
        Custom Exception class for Network Security
        :param error_message: Exception message
        :param error_details: sys module for traceback (optional)
        """
        super().__init__(error_message)  # Call base Exception class
        self.error_message = str(error_message)

        if error_details:
            exc_type, exc_obj, exc_tb = error_details.exc_info()

            if exc_tb is not None:
                self.lineno = exc_tb.tb_lineno
                self.file_name = exc_tb.tb_frame.f_code.co_filename
            else:
                self.lineno = "Unknown"
                self.file_name = "Unknown"
        else:
            self.lineno = "Unknown"
            self.file_name = "Unknown"

    def __str__(self):
        return f"Error occurred in script [{self.file_name}] at line [{self.lineno}]: {self.error_message}"
