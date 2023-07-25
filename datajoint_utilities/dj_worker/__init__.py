import argparse
import logging

from .worker import RegisteredWorker, WorkerLog, ErrorLog, DataJointWorker, logger


# arg-parser for usage as CLI

# combine different formatters
class ArgumentDefaultsRawDescriptionHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter
):
    pass


def parse_args(args):
    """
    Parse command line parameters
    :param args: command line parameters as list of strings (for example  ``["--help"]``)
    :type args: List[str]
    :return: `argparse.Namespace`: command line parameters namespace
    :rtype: obj
    """

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=ArgumentDefaultsRawDescriptionHelpFormatter
    )

    parser.add_argument("worker_name", help="Select the worker to run", type=str)

    parser.add_argument(
        "-d",
        "--duration",
        dest="duration",
        help="Run duration of the entire process",
        type=int,
        metavar="INT",
        default=None,
    )

    parser.add_argument(
        "-s",
        "--sleep",
        dest="sleep",
        help="Sleep time between subsequent runs",
        type=int,
        metavar="INT",
        default=None,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="Set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )

    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="Set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    return parser.parse_args(args)
