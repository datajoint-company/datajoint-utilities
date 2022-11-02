from logging import StreamHandler
import re
import datajoint as dj
import json


class PopulateHandler(StreamHandler):
    """
    Custom Log Handler to parse and handle DataJoint logs related to populating tables.
    """

    _patterns = ("Making", "Error making", "Success making")

    def __init__(
        self, notifiers, full_table_names, on_start=True, on_success=True, on_error=True
    ):
        """
        :param notifiers: list of instantiated "Notifier"
        :param full_table_names: list of full table names to get notified about
        :param on_start: (bool) notify on populate start (default=True)
        :param on_success: (bool) notify on populate finishes successfully (default=True)
        :param on_error: (bool) notify on populate errors out (default=True)
        """

        StreamHandler.__init__(self)
        assert all(hasattr(notifier, "notify") for notifier in notifiers)
        self.notifiers = notifiers
        self.full_table_names = full_table_names
        self._status_to_notify = {
            "start": on_start,
            "success": on_success,
            "error": on_error,
        }

    def emit(self, record):
        msg = self.format(record)
        if not any(p in msg for p in self._patterns):
            return
        match = re.search(
            r"(Making|Success making|Error making) (.*) -> (\S+)( - .*)?", msg
        )
        status, key_str, full_table_name, error_message = match.groups()

        key = json.loads(key_str.replace("'", '"'))
        error_message = error_message.replace(" - ", "") if error_message else ""

        status = {
            "Making": "START",
            "Success making": "SUCCESS",
            "Error making": "ERROR",
        }[status]

        if (
            not self._status_to_notify[status.lower()]
            or full_table_name not in self.full_table_names
        ):
            return

        schema_name, table_name = full_table_name.split(".")
        schema_name = schema_name.strip("`")
        table_name = dj.utils.to_camel_case(table_name.strip("`"))

        for notifier in self.notifiers:
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
