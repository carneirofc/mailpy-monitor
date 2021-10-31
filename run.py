#!/usr/bin/env python3
import argparse
import logging
import logging.config

import yaml

import app
import app.sms

if __name__ == "__main__":
    with open("app/logging.yaml", "r") as f:
        log_config = yaml.safe_load(f)

    logging.config.dictConfig(log_config)

    parser = argparse.ArgumentParser(
        description="Monitor PV EPICS values and if any of them isn't in a specified range, "
        "email a warning message to a list of targets."
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        help="start an unsecured SMTP connection and encrypt it using .starttls() (default: use SMTP_SSL)",
    )
    parser.add_argument(
        "--login",
        metavar="email@example.com",
        default="controle.supervisorio@gmail.com",
        help="define the sender email (default: controle.supervisorio@gmail.com)",
    )
    parser.add_argument(
        "-p",
        "--passwd",
        metavar="my_password",
        required=True,
        help="set the password used when trying to log in",
    )
    parser.add_argument(
        "-db",
        "--db_url",
        metavar="mongodb://localhost:27017/mailpy-db",
        default="mongodb://localhost:27017/",
        help="MongoDB connection URL",
    )

    args = parser.parse_args()

    # SMS
    sms_app = app.Manager(
        tls=args.tls,
        login=args.login,
        passwd=args.passwd,
        db_url=args.db_url,
    )

    sms_app.load_from_database()
    sms_app.start()
    sms_app.join()
