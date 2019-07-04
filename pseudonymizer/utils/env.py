from os import environ
import logging
import sys
from pseudonymizer.exceptions.Errors import NotFoundEnvironmentVariableError


def check_env():
    """Checks if mandatory environment variables exists on the system.
    If at least one of the following env variables doesn't exist,
    this will simply raise NotFoundEnvironmentVariableError
    and stop the program execution
    """
    try:
        if "SALT_KEY" not in environ:
            raise NotFoundEnvironmentVariableError("SALT_KEY")
        elif "DB_USER" not in environ:
            raise NotFoundEnvironmentVariableError("DB_USER")
        elif "DB_HOST" not in environ:
            raise NotFoundEnvironmentVariableError("DB_HOST")
        elif "DB_PASSWORD" not in environ:
            raise NotFoundEnvironmentVariableError("DB_PASSWORD")
        elif "CATALOG_PATH" not in environ:
            raise NotFoundEnvironmentVariableError("CATALOG_PATH")
        elif "PROCESS_PATH" not in environ:
            raise NotFoundEnvironmentVariableError("PROCESS_PATH")
        elif "DATA_DIR" not in environ:
            raise NotFoundEnvironmentVariableError("DATA_DIR")
        elif "ENVIRONMENT" not in environ:
            raise NotFoundEnvironmentVariableError("ENVIRONMENT")
    except NotFoundEnvironmentVariableError as e:
        logging.error(e)
        sys.exit(1)
