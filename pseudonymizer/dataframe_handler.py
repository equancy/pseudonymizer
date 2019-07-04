import pandas as pd
import numpy as np
import logging

# Sklearn not available on RHEL Franfinance.
# Comment this part as we don't need sklearn transformation
# from sklearn.preprocessing import MinMaxScaler, RobustScaler, MaxAbsScaler

from pseudonymizer.config.main import CATALOG
from pseudonymizer.transform import pseudonymize

logger = logging.getLogger(__name__)

CATALOG_METHOD = CATALOG['methods']


def pseudonymize_dataframe(df, data_catalog, hash_store, table_name):
    """Pseudonymize a pandas DataFrame based on Data Catalog information

    Parameters
    ----------
    df: pandas DataFrame to process
    data_catalog: DataCatalogManager instance
    hash_store: HashStoreManager instance
    table_name: table name

    Returns
    -------
    New pandas DataFrame processed according dictionary entry.
    """

    field_methods = data_catalog.get_fields_metadata(table_name)

    res_df = pd.DataFrame()
    for field in field_methods:
        if field_methods[field] == CATALOG_METHOD['DISCARD']:
            continue
        else:
            res_df[field] = process_serie(df[field],
                                          field_methods[field],
                                          )
            if field_methods[field] == CATALOG_METHOD['PSEUDONYMIZE']:
                serie_size = res_df[field].size
                hash_store.add_hashes(res_df[field],
                                      df[field],
                                      pd.Series(np.full(
                                          (serie_size),
                                          field)
                                          ),
                                      pd.Series(np.full(
                                          (serie_size),
                                          table_name)
                                          )
                                      )
    return res_df


def process_serie(serie, method='copy'):
    """Process a pandas Series according the method.

    Parameters
    ----------
    serie: a pandas Series()
    method: method to be applied to the serie

    Returns
    -------
    Series transformed with method
    """
    if method == CATALOG_METHOD['PASSTHROUGH']:
        return serie
    elif method == CATALOG_METHOD['PSEUDONYMIZE']:
        return serie.apply(pseudonymize)
    # elif 'transfo' in method:
    #     return rescale(serie,
    #                    min=TRANSFORMATIONS[method.upper()]['MIN'],
    #                    max=TRANSFORMATIONS[method.upper()]['MAX'],
    #                    scaler=TRANSFORMATIONS[method.upper()]['TYPE'],
    #                   )
    else:
        raise ValueError('Unknown method: {}'.format(method))


def rescale(serie, min=0, max=1, scaler='Robust'):
    """Rescale a pandas Series from min to max

    Parameters
    ----------
    serie: a pandas Series()
    min: minimal value for rescale range
    max: maximal value for rescale range
    scaler: name of scaler to use

    Returns
    -------
    a Series() rescaled
    """

    if scaler == 'Simple':
        return serie / max
    # elif scaler == 'MinMax':
    #     scaler = MinMaxScaler(feature_range=(min, max))        
    # elif scaler == 'Robust':
    #     scaler = RobustScaler(with_centering=False,
    #                           quantile_range=(0.05, 0.95))
    # elif scaler == 'MaxAbs':
    #     scaler = MaxAbsScaler()
    # else:
    #     raise ValueError('Unknown scaler for {}'.format(serie.name))
    # reshaped_serie = serie.astype(float).values.reshape(-1, 1)
    # return pd.Series(scaler.fit_transform(reshaped_serie).transpose()[0])


def depseudonymize_dataframe(df, process_catalog,
                             data_catalog, dst_table_name, hash_store):
    """Depseudonymize a pandas DataFrame based on both process and data catalog

    Parameters
    ----------
    df: DataFrame to process
    process_catalog_df: ProcessCatalogManager instance
    data_catalog_df: DataCatalogManager instance
    dst_table_name: Dest

    Returns
    -------
    DataFrame
    """

    process_dst_df = process_catalog.filter_dst_table(dst_table_name)
    src_table_df = data_catalog.catalog_df
    process_src_method_df = process_dst_df.merge(
        src_table_df,
        left_on=process_catalog.SRC_FIELDNAME_FIELD,
        right_on=data_catalog.FIELDNAME_FIELD,
        how='left'
        )

    all_fields = list(process_src_method_df[
        process_catalog.DST_FIELDNAME_FIELD
        ])

    for record in process_src_method_df.to_dict('records'):

        table = record[data_catalog.TABLE_FIELD]
        field = record[data_catalog.FIELDNAME_FIELD]

        dst_field = record[process_catalog.DST_FIELDNAME_FIELD]
        method = record[data_catalog.METHOD_FIELD]

        if field is np.nan or table is np.nan:
            df[dst_field] = df[dst_field]

        elif method == CATALOG_METHOD['PASSTHROUGH']:
            df[dst_field] = df[dst_field]

        elif method == CATALOG_METHOD['PSEUDONYMIZE']:
            hash_values_df = hash_store.query_partition_hash_df(table,
                                                                field=field)
            hash_values_df['is_valid'] = True
            df = df.merge(hash_values_df,
                          left_on=field,
                          right_on='HASH',
                          how='left'
                          )
            df['is_valid'] = df['is_valid'].fillna(False)
            if df['is_valid'].unique() != [True]:
                logger.warning('There were some problems')
                # Possibly raise error here

            df = df.drop([field], axis=1)
            df = df.rename(columns={"CLEAR": field})
            df = df[all_fields]

    return df
