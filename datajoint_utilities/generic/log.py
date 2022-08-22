import logging
import os
import sys
import typing as typ
from numbers import Number

import datajoint as dj

LogLevelName: typ.TypeAlias = typ.Literal[
    "CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"
]

LogLevelSource = typ.TypeAlias = typ.Literal["env", "cfg", "fallback"]


def get_formatter() -> logging.Formatter:
    """Get a custom logging formatter

    Returns:
        logging.Formatter: A logging formatter.
    """

    format = (
        "\n%(levelname)-8s | %(asctime)20s.%(msecs)-3d | PID=%(process)-7s | "
        "%(name)-20s %(funcName)20s\n%(message)s"
    )
    # print(format % dict(levelname="DEBUG", asctime="2022-08-19 09:44:08", msecs=12345, process=28542, name="something", funcName="<module>", message="('Setting database.host to localhost',)"))

    # format = (
    #     "\n{levelname:8} | {asctime:>20}.{msecs:0<3.0f} | PID={process:<7} | "
    #     "{name:20} {funcName:>20}\n{message}"
    # )
    # print(format.format(levelname="DEBUG", asctime="2022-08-19 09:44:08", msecs=12345, process=28542, name="something", funcName="<module>", message="('Setting database.host to localhost',)"))

    datetime = "%Y-%m-%d %H:%M:%S"

    return logging.Formatter(
        datefmt=datetime,
        fmt=format,
        style="%",
    )


def get_stderr_handler(
    formatter: None | logging.Formatter = None,
) -> logging.StreamHandler:  # type: ignore logging.StreamHandler[typ.TextIO]:
    """Get a stderr handler

    Returns:
        logging.StreamHandler: handler to stderr
    """
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setLevel("NOTSET")

    handler.setFormatter(formatter or get_formatter())
    return handler


def select_custom_loglevel(
    select: LogLevelSource = "env",
    env: str = "LOGLEVEL",
    cfg: str = "loglevel",
    fallback: LogLevelName = "WARNING",
) -> str:
    """Get a loglevel pulled from the environment or config file or fallback level.

    Args:
        select (typ.Literal["env", "cfg", "fallback"]): From where to source the loglevel. If the source is not available, will go down the priority list.
        env (str): Environment variable name used to get the loglevel from `os.getenv`.
        cfg (str): Config key name used to get the loglevel from `datajoint.config`.
        fallback (LogLevelName): Fallback loglevel if all other sources are not available.

    Returns:
        str: loglevel name as a string
    """
    order: dict[str, str] = {
        "env": os.getenv(env),
        "cfg": dj.config.get(cfg),  # type: ignore
        "fallback": fallback,
    }

    level_name = (
        order.get(select)
        or order.get("env")
        or order.get("cfg")
        or order.get("fallback")
    )

    return str(level_name).upper()


def get_loglevel(
    loglevel: str | int | None = None, base_level: LogLevelName = "WARNING"
) -> int:
    """Get the default loglevel

    Args:
        loglevel (str | int | None): A value that can be interpreted as a loglevel.
        base_level (str): Fallback or base level.

    Returns:
        int: loglevel as an integer

    Examples:
        Increased loglevel w/ verbosity flag:

            get_loglevel(len("vv")) -> 10
            get_loglevel(len("v"), base_level="INFO") -> 10
            get_loglevel(len("vvvvv"), base_level="CRITICAL") -> 10

        Get default loglevel (environment variable, config file, `base_level`):

            get_loglevel() -> ?
            get_loglevel(-1) ->  (ENV: LOGLEVEL)
            get_loglevel(-2) -> (`dj.config.get("loglevel")`)
            get_loglevel(-3) -> (`base_level`)

        Get loglevel from a string:

            get_loglevel("DEBUG")

        Get loglevel from an integer (starting at 10 and up):

            get_loglevel(10)
    """
    if loglevel is None or (isinstance(loglevel, int) and loglevel < 0):
        remap: dict[int, LogLevelSource] = {
            -1: "env",
            -2: "cfg",
            -3: "fallback",
        }
        try:
            loglevel = remap[loglevel or -1]
        except KeyError:
            loglevel = "env"
        return logging.getLevelName(select_custom_loglevel(loglevel))

    level_int = logging.getLevelName(base_level)
    loglevel_num = (
        int(loglevel)
        if isinstance(loglevel, Number)
        else logging.getLevelName(loglevel)
    )

    if loglevel_num < 10:
        return max(10, level_int - (10 * int(min(loglevel_num, level_int / 10))))

    return logging.getLevelName(logging.getLevelName(min(loglevel_num, 50)))


def remove_handlers(logger: logging.Logger) -> logging.Logger:
    """Remove all handlers from a logger

    Args:
        logger (logging.Logger): A logger
    """
    for handler in logger.handlers:
        logger.removeHandler(handler)
    return logger


def get_logger(
    name: str | None = "root",
    level: str | int | None = None,
    base_level: LogLevelName = "WARNING",
    handler: None | logging.Handler = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.hasHandlers() and handler is None:
        return logger
    level_num = get_loglevel(level, base_level)
    logger.setLevel(level_num)
    remove_handlers(logger).addHandler(handler or get_stderr_handler())
    return logger
