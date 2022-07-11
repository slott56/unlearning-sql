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
import toml


logger = logging.getLogger("application_name")


# class and def


def get_options(argv = sys.argv[1:]):
    parser = argparse.ArgumentParser(description="{{application_name}}")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    # parser.add_argument(...)
    # etc., for all arguments
    return parser.parse_args(argv)


def main(argv = sys.argv[1:], config=None):
    options = get_options()
    if options.verbose: 
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    logger.info("Starting {{application_name}}")
    
    # ... do the processing
    
    logger.info("Ending {{application_name}}")

    
if __name__ == "__main__":
    config_path = Path("template_config.toml")
    config = toml.load(config_path)
    logging.config.dictConfig(config['logging'])
    root = logging.getLogger("root")
    print(root, root.handlers)
    main(sys.argv[1:], config=config)
    logging.shutdown()
