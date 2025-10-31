from logging import StreamHandler
import re
import ast
import datajoint as dj
from typing import List, Any, Optional


class PopulateHandler(StreamHandler):
    """
    Log handler that parses DataJoint populate logs and sends notifications.
    
    Monitors log entries for "Making", "Success making", "Error making" patterns,
    extracts table names and keys, then notifies configured notifiers per table.
    Supports per-table notification settings and dynamic table addition.
    """

    _patterns = ("Making", "Error making", "Success making")

    def __init__(
        self, notifiers: List[Any], full_table_names: Optional[List[str]] = None, on_start: bool = True, on_success: bool = True, on_error: bool = True
    ) -> None:
        """
        :param notifiers: list of instantiated "Notifier"
        :param full_table_names: list of full table names to get notified about (optional)
        :param on_start: (bool) notify on populate start (default=True)
        :param on_success: (bool) notify on populate finishes successfully (default=True)
        :param on_error: (bool) notify on populate errors out (default=True)
        """

        StreamHandler.__init__(self)
        for notifier in notifiers:
            if not hasattr(notifier, "notify"):
                raise ValueError(f"Not a valid `Notifier` - missing `notify` method")
        self.notifiers = notifiers
        self.tables_to_notify: dict = {}
        if full_table_names is not None:
            for full_table_name in full_table_names:
                self.watch_table(full_table_name, on_start, on_success, on_error)

    def watch_table(self, full_table_name: str, on_start: bool = True, on_success: bool = True, on_error: bool = True) -> None:
        """
        Add a table to watch for populate notifications.
        
        :param full_table_name: Full table name (schema.table)
        :param on_start: Notify when populate starts (default=True)
        :param on_success: Notify when populate succeeds (default=True)
        :param on_error: Notify when populate fails (default=True)
        """
        self.tables_to_notify[full_table_name] = {
            "start": on_start,
            "success": on_success,
            "error": on_error,
        }

    def emit(self, record: Any) -> None:
        msg = self.format(record)
        if not any(p in msg for p in self._patterns):
            return
        match = re.search(
            r"(Making|Success making|Error making) (.*) -> (\S+)( - .*)?", msg
        )
        if not match:
            return
        status, key_str, full_table_name, error_message = match.groups()

        status = {
            "Making": "START",
            "Success making": "SUCCESS",
            "Error making": "ERROR",
        }[status]

        if self.tables_to_notify.get(full_table_name, {}).get(status.lower(), False):
            return

        try:
            key = ast.literal_eval(key_str)
            error_message = error_message.replace(" - ", "") if error_message else ""

            schema_name, table_name = full_table_name.split(".")
            schema_name = schema_name.strip("`")
            table_name = dj.utils.to_camel_case(table_name.strip("`"))
        except (ValueError, SyntaxError, AttributeError, KeyError, IndexError):
            # Skip malformed log entries silently
            return

        for notifier in self.notifiers:
            try:
                notifier.notify(
                    title=f"DataJoint populate - {schema_name}.{table_name} - {status}",
                    message=msg,
                    schema_name=schema_name,
                    table_name=table_name,
                    key=key,
                    status=status,
                    error_message=error_message,
                    **key,
                )
            except Exception as e:
                # Log notifier errors but continue with other notifiers
                print(f"PopulateHandler: Notifier failed: {e}")
