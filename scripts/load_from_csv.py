#!/usr/bin/env python3
import argparse
import logging
import logging.config
import os
import sys

import yaml

logger = logging.getLogger()

if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, f"{here}/..")
    with open(f"{here}/../app/logging.yaml", "r") as f:
        log_config = yaml.safe_load(f)
        logging.config.dictConfig(log_config)

    parser = argparse.ArgumentParser(
        description="Load entries and groups from a csv file"
    )
    parser.add_argument(
        "file",
    )
    parser.add_argument(
        "--url", help="DB connection url", default="mongodb://localhost:27017/mailpy-db"
    )
    args = parser.parse_args()

    import app.utility

    # Connect
    logger.info(f"Connecting to database at {args.url}")
    app.utility.connect(url=args.url)

    # Create entries from a csv file
    csv_file = os.path.abspath(args.file)
    logger.info(f"Loading data from {csv_file}")
    app.utility.load_csv_table(f"{csv_file}")

    # Disconnect
    logger.info("Operation finished")
