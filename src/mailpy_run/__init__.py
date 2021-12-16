#!/usr/bin/env python3
import argparse

import mailpy.logging as logging

logger = logging.getLogger()


def start_test_database():
    parser = argparse.ArgumentParser("Start a mongodb container for testing purposes")
    parser.parse_args()

    import mailpy.tools

    logging.load_config_console()

    parser = argparse.ArgumentParser(
        description="Start a dummy mongodb with some test data"
    )
    parser.parse_args()
    m = mailpy.tools.MongoContainerManager()
    print(f"Stating mongodb container based on {m.config.image}")
    print(
        f"connection string: 'mongodb://{m.config.username}:{m.config.password}@{m.config.host}:{m.config.port}/{m.config.database}'"
    )
    m.start()


def start_alarm_server():
    import mailpy.manager

    parser = argparse.ArgumentParser(
        description="Monitor PV EPICS values and if any of them isn't in a specified range, "
        "email a warning message to a list of targets."
    )
    # --------- General Settings
    parser.add_argument(
        "--logging-config",
        help="yml config file for logging",
        dest="logging_config",
    )
    parser.add_argument(
        "-db",
        "--db_url",
        metavar="mongodb://localhost:27017/mailpy",
        default="mongodb://localhost:27017/mailpy",
        help="MongoDB connection URL",
    )
    # --------- Mail Server Settings
    parser.add_argument(
        "--mail-server-port",
        dest="email_server_port",
        help="Email server port. eg: 25",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--mail-server-host",
        dest="email_server_host",
        help="Email server hostname or IP address. eg: 'smtp.gmail.com'",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        help="start an unsecured SMTP connection and encrypt it using .starttls() (default: use SMTP_SSL)",
    )
    # --------- Mail Client Settings
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

    args = parser.parse_args()
    logging_config = args.logging_config
    if not logging_config:
        print(
            f"logging_config '{logging_config}' setting is empty, using the default rotating file config"
        )
        logging.load_config_rotating_file()

    # SMS
    sms_app = mailpy.manager.Manager(
        config=mailpy.manager.Config(
            email_server_host=args.email_server_host,
            email_server_port=args.email_server_port,
            email_tls_enabled=args.tls,
            email_login=args.login,
            email_password=args.passwd,
            db_connection_string=args.db_url,
        )
    )
    sms_app.initialize_entries_from_database()
    sms_app.start()
    sms_app.join()
