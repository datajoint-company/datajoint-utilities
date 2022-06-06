from logging import StreamHandler
import re
import datajoint as dj


class PopulateHandler(StreamHandler):
    """
    Custom Log Handler to parse and handle DataJoint logs related to populating tables.
    """

    _patterns = ('Start populating TABLE',
                 'Successful in populating TABLE',
                 'Error in populating TABLE')

    def __init__(self, notifiers, full_table_names,
                 on_start=True,
                 on_success=True,
                 on_error=True):
        """
        :param notifiers: list of instantiated "Notifier"
        :param full_table_names: list of full table names to get notified about
        :param on_start: (bool) notify on populate start (default=True)
        :param on_success: (bool) notify on populate finishes successfully (default=True)
        :param on_error: (bool) notify on populate errors out (default=True)
        """

        StreamHandler.__init__(self)
        assert all(hasattr(notifier, 'notify') for notifier in notifiers)
        self.notifiers = notifiers
        self.full_table_names = full_table_names
        self._status_to_notify = {'start': on_start, 'success': on_success, 'error': on_error}

    def emit(self, record):
        msg = self.format(record)
        if not any(p in msg for p in self._patterns):
            return
        print(msg)
        match = re.search(r'(Start|Success|Error).*populating TABLE: (.*) - KEY.*', msg)
        status, full_table_name = match.groups()

        if (not self._status_to_notify[status.lower()]
                or full_table_name not in self.full_table_names):
            return

        schema_name, table_name = full_table_name.split(".")
        schema_name = schema_name.strip("`")
        table_name = dj.utils.to_camel_case(table_name.strip("`"))
        table_name = f"{schema_name}.{table_name}"

        for notifier in self.notifiers:
            notifier.notify(title=f'DataJoint populate - {table_name} - {status.upper()}', message=msg)
