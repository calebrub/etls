"""
logging_utils.py
~~~~~~~~~~~~~~~~
Shared logging helpers used by generate_identifiers.py and fetch_and_load_reports.py.

TeeStream mirrors every write to both a file and the original stream (console), so
all output — whether from print() or the logging module — lands in both places.
setup_file_logging wires the root logger and redirects sys.stdout/sys.stderr to a
TeeStream before any other module-level code runs.
"""

import logging
import os
import sys


class TeeStream:
    """
    A write-through proxy that forwards every write to both a file and the
    original stream (e.g. the real sys.stdout / sys.stderr).

    The file is opened in append mode so successive runs accumulate in the
    same log file rather than overwriting it.

    Console output is capped to avoid n8n/process buffer overflow errors.
    """

    def __init__(self, filename: str, stream, max_console_bytes: int = 800000):
        self.stream = stream
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.file = open(filename, 'a', encoding='utf-8')
        self.max_console_bytes = max_console_bytes
        self.bytes_written = 0
        self.truncated = False

    def write(self, message: str) -> None:
        self.file.write(message)
        self.file.flush()

        if not self.truncated:
            try:
                msg_bytes = len(message.encode('utf-8', errors='ignore'))
            except Exception:
                msg_bytes = len(message)
                
            if self.bytes_written + msg_bytes > self.max_console_bytes:
                self.stream.write("\n... [CONSOLE LOGS TRUNCATED TO PREVENT BUFFER OVERFLOW - SEE FULL LOGS IN FILE] ...\n")
                self.stream.flush()
                self.truncated = True
            else:
                self.stream.write(message)
                self.bytes_written += msg_bytes

    def flush(self) -> None:
        self.stream.flush()
        self.file.flush()


def setup_file_logging(log_filename: str) -> None:
    """
    Redirect sys.stdout and sys.stderr to TeeStream instances so that all
    output (print statements and logging calls) is written to both the
    console and a log file under the ``logs/`` directory next to the script
    that called this function.

    Must be called before any other imports that produce output.
    """
    # Resolve the logs directory relative to the calling script's location
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    log_dir = os.path.join(script_dir, "logs")
    log_path = os.path.join(log_dir, log_filename)

    sys.stdout = TeeStream(log_path, sys.stdout)
    sys.stderr = TeeStream(log_path, sys.stderr)

    # Root logger writes through the redirected stdout so every logging.*
    # call also ends up in the log file.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )
