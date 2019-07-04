
from ast import literal_eval
from os import environ, path
from pseudonymizer.utils.env import check_env

check_env()

conn_str_tmpl = "postgres://{db_user}:{db_password}@{db_host}:{db_port}"

ENVIRONMENT = environ.get('ENVIRONMENT')
SALT_KEY = environ.get('SALT_KEY')

tmp_cln_input = environ.get('CLEAN_INPUT_FOLDER')
CLEAN_INPUT_FOLDER = literal_eval(tmp_cln_input) if tmp_cln_input else False

tmp_cln_lksh = environ.get('CLEAN_LAKESHORE_FOLDER')
CLEAN_LAKESHORE_FOLDER = literal_eval(tmp_cln_lksh) if tmp_cln_lksh else False

HASH_DB = {
    "conn_string": conn_str_tmpl.format(db_user=environ.get('DB_USER'),
                                        db_password=environ.get('DB_PASSWORD'),
                                        db_host=environ.get('DB_HOST'),
                                        db_port=environ.get('DB_PORT', 5432)),
    "table": "frfi_hashkey_store"
}

DATA_DIR = {
    "INPUT_DIR": path.join(environ.get('DATA_DIR'), "input"),
    "ARCHIVE_DIR": path.join(environ.get('DATA_DIR'), "archive"),
    "RAW_DIR": path.join(environ.get('DATA_DIR'), "raw"),
    "LAKESHORE_DIR": path.join(environ.get('DATA_DIR'), "lakeshore"),
    "LAKESHORE_PII_DIR": path.join(environ.get('DATA_DIR'), "lakeshore_pii")
}

CATALOG = {
    "path": environ.get('CATALOG_PATH'),
    "params": {
        "sheet_name": environ.get('CATALOG_SHEET_NAME'),
        "header_row": int(environ.get('CATALOG_HEADER_ROW', 0)),
        "source_field": "Source",
        "table_field": "Table",
        "fieldname_field": "Champ",
        "method_field": "Traitement RGPD",
        "state_field": "Actif",
    },
    "methods": {
        "DISCARD": "ne pas stocker",
        "PASSTHROUGH": "en clair",
        "PSEUDONYMIZE": "pseudonymisation",
    }
}

PROCESS = {
    "path": environ.get('PROCESS_PATH'),
    "params": {
        "sheet_name": environ.get('PROCESS_SHEET_NAME'),
        "header_row": int(environ.get('PROCESS_HEADER_ROW', 0)),
        "dst_table_field": "USE CASE TABLE",
        "dst_fieldname_field": "USE CASE FIELD",
        "src_table_field": "SOURCE TABLE",
        "src_fieldname_field": "SOURCE FIELD"
    },
}

TRANSFORMATIONS = [
  {"TRANSFO": "transfo2"},
  {"TRANSFO": "transfo1"}
]