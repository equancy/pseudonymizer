from glob import glob
import logging
import os
import sys
import click
import warnings

import pandas as pd

from pseudonymizer.data_catalog import (DataCatalogManager,
                                        ProcessCatalogManager)
from pseudonymizer.config.main import (CATALOG,
                                       PROCESS,
                                       DATA_DIR,
                                       ENVIRONMENT,
                                       CLEAN_INPUT_FOLDER,
                                       CLEAN_LAKESHORE_FOLDER)

from pseudonymizer.utils.files import (get_table_name,
                                       get_filename,
                                       write_df_to_csv)

from pseudonymizer.hash_store import HashStoreManager
from pseudonymizer.dataframe_handler import (pseudonymize_dataframe,
                                             depseudonymize_dataframe)

from pseudonymizer.exceptions.Errors import TableNotFoundInCatalogException

logger_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log_level = logging.INFO if ENVIRONMENT == 'production' else logging.DEBUG
logging.basicConfig(level=log_level, format=logger_format)
logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)


@click.command()
@click.option('-r', '--reverse',
              help='Use reverse in case you want to depseudonymize files',
              is_flag=True)
@click.option('-i', '--init',
              help='Initialise hash table',
              is_flag=True)
def main(reverse, init):

    data_catalog = DataCatalogManager(CATALOG['path'], params=CATALOG['params'])
    hash_store = HashStoreManager()

    def pseudonymize():
        input_csv_files_path = DATA_DIR['INPUT_DIR'] + "**/*.csv"
        files = [f for f in glob(input_csv_files_path, recursive=True)]

        if files == []:
            logger.info('No files found '
                        'in input directory {}.'.format(DATA_DIR['INPUT_DIR']))

        logger.debug('Start looping on each file to pseudonymize')
        for f in files:
            table_name = get_table_name(f, '([a-zA-z]+)_\d+\d+\d+.csv')
            filename = get_filename(f, '([a-zA-z]+_\d+\d+\d+.csv)')

            try:
                field_names = data_catalog.get_fields_metadata(table_name,
                                                               asList=True)
            except TableNotFoundInCatalogException:
                # Pass
                continue

            logger.debug('Bread')
            in_df = pd.read_csv(f,
                                sep=",",
                                header=None,
                                names=field_names)

            logger.debug('Start dataframe pseudonymization')
            raw_df = pseudonymize_dataframe(in_df,
                                            data_catalog,
                                            hash_store,
                                            table_name)
            logger.debug('Get raw file path')
            raw_f_path = os.path.join(DATA_DIR['RAW_DIR'],
                                      filename)

            logger.debug('Writing file to raw folder')
            write_df_to_csv(raw_df, raw_f_path)

            logger.debug('Get archive file path')
            archive_f_path = os.path.join(DATA_DIR['ARCHIVE_DIR'],
                                          filename)

            logger.debug('Writing untouched file to archive folder')
            write_df_to_csv(in_df, archive_f_path)

            if CLEAN_INPUT_FOLDER:
                # Clean up INPUT_DIR files
                os.remove(f)

    def depseudonymize():
        process_catalog = ProcessCatalogManager(PROCESS['path'],
                                                params=PROCESS['params'])
        lakeshore_csv_files_path = DATA_DIR['LAKESHORE_DIR'] + "/*.csv"
        files = [f for f in glob(lakeshore_csv_files_path, recursive=True)]

        if files == []:
            logger.info('No files found '
                        'in lakeshore '
                        'directory {}.'.format(DATA_DIR['LAKESHORE_DIR']))

        logger.debug('Start looping on each file to depseudonymize')
        for f in files:
            table_name = get_table_name(f, '([a-zA-z]+)_\d+\d+\d+.csv')
            filename = get_filename(f, '([a-zA-z]+_\d+\d+\d+.csv)')

            field_names = process_catalog.get_fields_metadata(table_name,
                                                              asList=True)

            try:
                field_names = process_catalog.get_fields_metadata(table_name,
                                                                  asList=True)
            except TableNotFoundInCatalogException:
                # Pass
                continue

            logger.debug('Before in_df read')
            in_df = pd.read_csv(f,
                                sep=",",
                                header=None,
                                names=field_names)

            lakeshore_pii_df = depseudonymize_dataframe(in_df,
                                                        process_catalog,
                                                        data_catalog,
                                                        table_name,
                                                        hash_store)

            logger.debug('Get lakeshore pii file path')
            lakeshore_pii_f_path = os.path.join(DATA_DIR['LAKESHORE_PII_DIR'],
                                                filename)

            logger.debug('Write depseudonymized file to lakeshore pii folder')
            write_df_to_csv(lakeshore_pii_df, lakeshore_pii_f_path)

            if CLEAN_LAKESHORE_FOLDER:
                # Clean up LAKESHORE_DIR files
                os.remove(f)

    if init:
        logger.debug('Init hash table')
        hash_store.init_reset_hash_table()
        sys.exit(0)

    if reverse:
        depseudonymize()
    else:
        pseudonymize()

    # Close connection pools
    hash_store.dispose()
