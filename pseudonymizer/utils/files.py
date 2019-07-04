
import logging
import re

logger = logging.getLogger(__name__)


def get_table_name(file_path, regex):
    """
    Extract table name from file path

    Parameters
    ----------
    file_path: file absolute path
    regex: Matching rule to extract table name

    Returns
    -------
    String
    """
    regex = '([a-zA-z]+)_\d+\d+\d+.csv'

    try:
        logger.debug('Get table name from {}'.format(file_path))
        return re.search(regex, file_path).group(1)
    except AttributeError:
        logger.error('{fp} not matching regex {rgx}'.format(fp=file_path,
                                                            rgx=regex))
        pass


def get_filename(file_path, regex):
    """
    Extract file name from file path

    Parameters
    ----------
    file_path: file absolute path
    regex: Matching rule to extract table name

    Returns
    -------
    String
    """
    try:
        logger.debug('Get filename from {}'.format(file_path))
        return re.search(regex, file_path).group(1)
    except AttributeError:
        logger.error('{fp} not matching regex {rgx}'.format(fp=file_path,
                                                            rgx=regex))
        pass


def write_df_to_csv(df, write_path):
    """Write DataFrame in disk at given path
    Parameters
    ----------
    write_path: file path to write ons

    Returns
    -------
    None
    """
    logger.info("Writing {}".format(write_path))
    df.to_csv(
        write_path,
        sep=",",
        encoding="utf-8",
        index=False,
        header=False
    )
