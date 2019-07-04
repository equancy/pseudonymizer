class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class NotFoundEnvironmentVariableError(Error):
    """Raise when environment variable is missing"""
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str("Missing env variable : " + self.message)


class NoDatacatalogMatchRegexException(Error):
    """Raise no matching can be done between file in data folder and regex"""
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str("No matching found for " + self.message)


class TableNotFoundInCatalogException(Error):
    """Raise when table lookup in data catalog failed"""
    def __init__(self, table):
        self.table = table

    def __str__(self):
        return str("Table " + self.table + " cannot be found in Data Catalog")
