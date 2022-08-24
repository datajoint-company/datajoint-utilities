"""General command-line utilities."""

import argparse
from typing import Optional, Sequence

from datajoint_utilities.version import __version__


class HelpFmtDefaultsDocstring(
    argparse.RawDescriptionHelpFormatter,
    argparse.ArgumentDefaultsHelpFormatter,
):
    """Combination of different argparse help formatters.

    - Use top-level docstring from module in help
    - Show default values in help
    """

    pass


class HelpFmtDefaultsDocstringMeta(
    argparse.RawDescriptionHelpFormatter,
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.MetavarTypeHelpFormatter,
):
    """Combination of different argparse help formatters.

    - Use top-level docstring from module in help
    - Show default values in help
    - Show types as MetaVar values in help
    """

    pass


class MultiplyArg(argparse.Action):
    """Custom action to multiply positive values of a user defined option."""

    def __init__(self, option_strings, dest, multiplier=1, nargs=None, *args, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super().__init__(option_strings, dest, *args, **kwargs)
        self.multiplier = multiplier

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str,
        option_string: str = "",
    ):
        num = float(values)
        setattr(namespace, self.dest, self.multiplier * num if num > 0.0 else values)


class CommaSepArgs(argparse.Action):
    """Split comma-separated input arguments."""

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super().__init__(option_strings, dest, nargs="*", **kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: list[str],
        option_string: str = "",
    ):
        if not option_string:
            return
        comma_splits = []
        [comma_splits.extend(string.split(",")) for string in values]
        whitespace_stripped = [string.strip() for string in comma_splits]
        keywords = getattr(namespace, self.dest) or []
        [keywords.extend(string.split()) for string in whitespace_stripped]
        setattr(namespace, self.dest, list(filter(None, set(keywords))))


class EnvVarArgs(argparse.Action):
    """Split KEY=VALUE input arguments."""

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super().__init__(option_strings, dest, **kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str,
        option_string: str = "",
    ):
        if not option_string:
            return
        keyval = values.split("=", 1)
        key = keyval.pop(0)
        if not key:
            return
        val = keyval.pop() if keyval else ""
        kwargs = getattr(namespace, self.dest)
        kwargs = {} if kwargs is None else kwargs
        kwargs |= {key: val}
        setattr(namespace, self.dest, kwargs)


class ArgparseBase:
    def __init__(
        self,
        sysargv: Sequence[str],
        name: str,
        version: Optional[str] = None,
        description: Optional[str] = None,
        formatter: Optional[argparse.HelpFormatter] = None,
    ) -> None:
        """Parse sys argument list using `parse_args` method.

        Args:
            sysargv (Sequence[str]): List of arguments to be passed in.
            formatter (Optional[cmd.T_ArgFmts], optional): A class passed to the
                `formatter_class=` argument in `argparse.ArgumentParser()`.
                Defaults to `argparse.RawDescriptionHelpFormatter`.
        """
        cli_version = version if version is not None else __version__
        help_description = (
            description if description is not None else "DataJoint command-line utility"
        )
        help_formatter = formatter if formatter is not None else argparse.HelpFormatter

        self.sys_args: Sequence[str] = sysargv
        self.parser = argparse.ArgumentParser(
            prog=name,
            description=str(help_description),
            formatter_class=help_formatter,
            allow_abbrev=False,
        )

        self.parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=f"%(prog)s {cli_version}",
        )

        self.parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=0,
            help="increase logging verbosity by specifying multiple",
        )

        self.make()
        self.namespace = self.parser.parse_args(self.sys_args or ["-h"])
        self._vars = vars(self.namespace)

    def make(self):
        """Add components to the argument parser `self.parser`."""
        pass

    @property
    def args(self):
        return self._vars
