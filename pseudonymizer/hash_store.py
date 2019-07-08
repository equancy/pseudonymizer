import pandas as pd
import logging
import sqlalchemy as db
from sqlalchemy.types import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from pseudonymizer.config.main import HASH_DB, ENVIRONMENT
from pseudonymizer.utils.tools import batcher

Base = declarative_base()

logger = logging.getLogger(__name__)

HASH_FIELD = 'HASH'
CLEAR_FIELD = 'CLEAR'
TABLE_FIELD = 'TABLE'
FIELDNAME_FIELD = 'FIELD'


class HashStoreModel(Base):
    """Hash Store DB Model
    """
    __tablename__ = HASH_DB['table']
    hash = db.Column(HASH_FIELD, String)
    clear = db.Column(CLEAR_FIELD, String)
    table = db.Column(TABLE_FIELD, String)
    field = db.Column(FIELDNAME_FIELD, String)
    __table_args__ = (
        db.Index('hash_idx', HASH_FIELD),
        db.PrimaryKeyConstraint(TABLE_FIELD,
                                FIELDNAME_FIELD,
                                HASH_FIELD,
                                name='table_field_hash_tree_pk')
        )


class HashStoreManager():
    """Hash store manager wrapper class of Hash Store
    """
    def __init__(self):
        """
        """
        
        self.engine = db.create_engine(HASH_DB['conn_string'])
        self.session_maker = scoped_session(sessionmaker(bind=self.engine,
                                                         autocommit=False))
        logger.info('Initialize hash store engine')

    def add_hashes(self, hash_serie, clear_serie, table_name, field_name):
        """Update hash table with newly hashed data

        Parameters
        ----------
        hash_serie=: Pandas Serie with hashed values
        clear_serie=: Pandas Serie with clear values
        table_name=: partition table name
        field_name=: partition field name

        Returns
        -------
        None
        """
        # Initialize session object
        db_session = self.session_maker()

        # rebuild DataFrame from Series objects
        df = pd.DataFrame({HASH_FIELD: hash_serie,
                           CLEAR_FIELD: clear_serie})
        hash_records = df.to_dict(orient='records')

        # Create array of arrays containing at most 100 records
        hash_records_batches = batcher(hash_records, 100)

        for hash_records_batch in hash_records_batches:
            for hash_object in hash_records_batch:
                hash_inst = HashStoreModel(hash=hash_object[HASH_FIELD],
                                           clear=hash_object[CLEAR_FIELD],
                                           table=table_name,
                                           field=field_name)
                db_session.merge(hash_inst)
            db_session.commit()
        db_session.close()

    def get_hash_df(self):
        """Read all hash store

        Returns
        -------
        DataFrame
        """
        # Check if table exists, use a empty dataframe if not
        if not self.engine.dialect.has_table(self.engine, HASH_DB['table']):
            return pd.DataFrame(data={HASH_FIELD: [],
                                      CLEAR_FIELD: [],
                                      TABLE_FIELD: [],
                                      FIELDNAME_FIELD: []})
        else:
            return pd.read_sql_table(HASH_DB['table'], self.engine)

    def query_hash_df(self, sql_query):
        """Query hash store using sql_query.
        Parameters
        ----------
        sql_query=: string SQL query or SQLAlchemy Selectable (select or text object) SQL query to be executed.

        Returns
        -------
        DataFrame
        """
        return pd.read_sql_query(sql_query, self.engine)

    def query_partition_hash_df(self, table, field=None):
        """ Query hash store by table partition (+ field partition optionnaly)
        This is useful when you need to query fetch information from hash store filtered by table/field

        Parameters
        ----------
        table=: table partition name
        field: field partition name

        Returns
        -------
        DataFrame filtered by table / field
        """
        metadata = db.MetaData()
        hash_table = db.Table(HASH_DB['table'],
                              metadata,
                              autoload=True,
                              autoload_with=self.engine)
        hash_columns = hash_table.columns
        where_clause = ''
        if field is None:
            where_clause = hash_columns.TABLE == table
        else:
            where_clause = db.and_(
                hash_columns.TABLE == table,
                hash_columns.FIELD == field
                )

        sql_query = db.select([hash_table]).where(where_clause)
        return self.query_hash_df(sql_query)

    def dispose(self):
        """Clean up engine connection
        """
        # Dispose the engine
        self.engine.dispose()

    def init_reset_hash_table(self):
        """ Convenient method to reset hash table
        This should be executed only in devel environment
        """
        if ENVIRONMENT == 'devel':
            Base.metadata.drop_all(self.engine)
            Base.metadata.create_all(self.engine)
        else:
            logger.warning('Reset hash table is not recommended '
                           'anywhere else than devel environment')
