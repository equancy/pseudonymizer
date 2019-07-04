import pandas as pd
import numpy as np
import os
import logging
from pseudonymizer.exceptions.Errors import TableNotFoundInCatalogException

logger = logging.getLogger(__name__)


# TODO Catalog Manager
# Subclass DataCatalog + Process Manager

class DataCatalogManager:
    """Data Catalog wrapper class.
    """
    def __init__(self, xslx_path, params=None):
        """
        Parameters
        ----------
        xslx_path=: Absolute path to where
        hashed_serie: hashed values

        Returns
        -------
        None
        """
        logger.info('Loading Datacatalog: %s', xslx_path)
        self.path = xslx_path
        self.sheet_name = params.get('sheet_name', None)
        self.header_row = params.get("header_row", None)
        if self.path is None:
            msg = "xslx_path path expected for DatacatalogManager"
            raise Exception(msg)
        elif os.path.isabs(self.path) is not True:
            msg = "xslx_path Must be absolute path to Data Catalog file"
            raise Exception(msg)

        self.catalog_df = pd.read_excel(self.path,
                                        sheet_name=self.sheet_name,
                                        header=self.header_row)
        self.SOURCE_FIELD = params['source_field']
        self.TABLE_FIELD = params['table_field']
        self.FIELDNAME_FIELD = params['fieldname_field']
        self.METHOD_FIELD = params['method_field']
        self.STATE_FIELD = params['state_field']
        logger.info('Building catalog')
        self.catalog_dict = self.build_data_catalog()

        # Need to filter catalog_df as well
        self.catalog_df = self.catalog_df[
                (self.catalog_df[self.STATE_FIELD].str.lower() == "oui") |
                (self.catalog_df[self.STATE_FIELD].str.lower() == "yes")
            ]
        logger.info('Catalog built with success')

    def build_data_catalog(self):
        """Build a data catalog as a list of dictionaries from a pandas data frame.
        @TODO
        """
        catalog_collection = (self.catalog_df[
                (self.catalog_df[self.STATE_FIELD].str.lower() == "oui") |
                (self.catalog_df[self.STATE_FIELD].str.lower() == "yes")
            ]
            # Regrouping lines per source
            .groupby(self.TABLE_FIELD)
            [[self.FIELDNAME_FIELD, self.METHOD_FIELD]]
            # Building fields dictionary from grouped lines, lower casing method
            .apply(lambda _g: dict(
                zip(_g[self.FIELDNAME_FIELD], _g[self.METHOD_FIELD].str.lower()))
                )
            .reset_index()
            # Get table field & source field
            .merge(self.catalog_df[[self.TABLE_FIELD, self.SOURCE_FIELD]].drop_duplicates(),
                   on=self.TABLE_FIELD)
            # Deal with NaN values (mostly sheetname) from reading Excel file
            .replace({np.nan: None})
            # Rename for processing
            .rename({self.SOURCE_FIELD: 'source', 0: 'fields'}, axis=1)
            # Return a list of records (as dict)
            .to_dict(orient='records')
        )
        return {elem[self.TABLE_FIELD]: elem for elem in catalog_collection}

    def get_fields_metadata(self, table_name, asList=False):
        """@TODO
        """
        table = None

        try:
            table = self.catalog_dict[table_name]
        except KeyError:
            logger.error('{} is not present in datacatalog'.format(table_name))
            raise TableNotFoundInCatalogException(table_name)

        if asList:
            return list(table['fields'].keys())

        return table['fields']


class ProcessCatalogManager:
    """Process Catalog wrapper class.
    """
    def __init__(self, xslx_path, params=None):
        logger.info('Loading Datacatalog Process: %s', xslx_path)
        self.path = xslx_path
        self.sheet_name = params.get('sheet_name', None)
        self.header_row = params.get("header_row", 0)

        if self.path is None:
            msg = "xslx_path path expected for ProcessCatalogManager"
            raise Exception(msg)
        elif os.path.isabs(self.path) is not True:
            msg = "xslx_path Must be absolute path to Process Catalog file"
            raise Exception(msg)

        self.DST_TABLE_FIELD = params['dst_table_field']
        self.DST_FIELDNAME_FIELD = params['dst_fieldname_field']
        self.SRC_TABLE_FIELD = params['src_table_field']
        self.SRC_FIELDNAME_FIELD = params['src_fieldname_field']
        self.catalog_df = pd.read_excel(self.path,
                                        sheet_name=self.sheet_name,
                                        header=self.header_row
                                        )
        logger.info('Process Catalog built with success')

    def get_fields_metadata(self, table_name, asList=False):
        """@TODO
        """
        table = None

        try:
            table = self.catalog_df[
                self.catalog_df[self.DST_TABLE_FIELD] == table_name
                ]
        except KeyError:
            logger.error('{} is not present in datacatalog'.format(table_name))
            raise TableNotFoundInCatalogException(table_name)

        if asList:
            return list(table[self.DST_FIELDNAME_FIELD])

        return table[self.DST_FIELDNAME_FIELD]

    def filter_dst_table(self, table_name):
        """@TODO
        """
        return self.catalog_df[
            self.catalog_df[self.DST_TABLE_FIELD] == table_name
            ]
