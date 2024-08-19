"""
{{application_name}}.
Docstring explaining what this is.

Use functions and classes.
Avoid billboard comments like this:
--------------------------------
-- Step 3a. Do The Thing      --
-- This prepares for step 3b. --
-- It follows step 2.         --
--------------------------------
"""

# imports
import argparse
import logging
import logging.config
from pathlib import Path
import sys
import tomllib
from typing import Any


logger = logging.getLogger("application_name")


# class and def


def get_options(
    config: dict[str, Any], argv: list[str] = sys.argv[1:]
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="{{application_name}}")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    # parser.add_argument(...)
    # etc., for all arguments
    return parser.parse_args(argv)


def main(options: argparse.Namespace) -> None:
    logger.info("Starting {{application_name}}")

    # ... do the processing

    logger.info("Ending {{application_name}}")


if __name__ == "__main__":
    config_path = Path("template_config.toml")
    config = tomllib.loads(config_path.read_text())
    logging.config.dictConfig(config["logging"])
    root = logging.getLogger("root")
    options = get_options(config, sys.argv[1:])
    if options.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    main(options)
    logging.shutdown()
