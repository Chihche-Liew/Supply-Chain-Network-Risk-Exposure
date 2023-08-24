import pandas as pd
from tqdm import tqdm
import datetime as dt
import duckdb
import wrds
import cleanco
import multiprocessing
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
db = wrds.Connection(wrds_username='lzy2lzz')
duckdb.default_connection.execute("SET GLOBAL pandas_analyze_sample=100000")

factset_sc = pd.read_csv('./supply_chain/factset_sc.csv')
factset_sc = factset_sc[['start_', 'end_', 'rel_type', 'SOURCE_name', 'source_company_id', 'SOURCE_cusip', 'SOURCE_isin', 'TARGET_name', 'target_company_id', 'TARGET_cusip', 'TARGET_isin']]
factset_sc.loc[factset_sc['end_'] == '4000-01-01 00:00:00', 'end_'] = dt.date(2023, 12, 31)
factset_sc['end_'] = pd.to_datetime(factset_sc['end_'])
factset_sc['start_'] = pd.to_datetime(factset_sc['start_'])

sc = pd.DataFrame()
sc['source_company_id'] = factset_sc['source_company_id'].drop_duplicates()
sc['lnk'] = 1
year = pd.DataFrame(
    {'year': range(2003, 2024)}
)
year['lnk'] = 1
sc = sc.merge(year,
              how='left',
              on='lnk')

quarter = pd.DataFrame(
    {'quarter': [1, 2, 3, 4],
     'lnk': [1, 1, 1, 1]}
)
sc = sc.merge(quarter,
              how='left',
              on='lnk')
del year
del quarter
del sc['lnk']
sc['date'] = pd.np.select([sc['quarter'] == 1,
                           sc['quarter'] == 2,
                           sc['quarter'] == 3,
                           sc['quarter'] == 4],
                          ['3-31', '6-30', '9-30', '12-31'])
sc['date'] = sc.apply(lambda x: str(x['year']) + '-' + x['date'], axis=1)
sc['date'] = pd.to_datetime(sc['date'])


sc = duckdb.query("""
SELECT a.*, b.*
FROM sc as a
LEFT JOIN factset_sc as b
ON a.source_company_id = b.source_company_id and a.date >= b.start_ and a.date <= b.end_
""").df()
sc = sc.drop(['source_company_id_2', 'start_', 'end_'], axis=1)
sc = sc.loc[sc['target_company_id'].notna()]
sc['SOURCE_cusip'] = sc['SOURCE_cusip'].str[:8]
sc['TARGET_cusip'] = sc['TARGET_cusip'].str[:8]
sc = sc.loc[((sc['SOURCE_cusip'].notna()) | (sc['SOURCE_name'].notna()) | (sc['SOURCE_isin'].notna())) &
            ((sc['TARGET_cusip'].notna()) | (sc['TARGET_name'].notna()) | (sc['TARGET_isin'].notna())) ]
sc['SOURCE_name'] = sc['SOURCE_name'].apply(lambda x: cleanco.basename(x.lower()))
sc['TARGET_name'] = sc['TARGET_name'].apply(lambda x: cleanco.basename(x.lower()))
supplier = sc.loc[sc['rel_type'] == 'SUPPLIER']
customer = sc.loc[sc['rel_type'] == 'CUSTOMER']

supplier_cusip = supplier.loc[(supplier['SOURCE_cusip'].notna()) & (supplier['TARGET_cusip'].notna())]
supplier = supplier.drop(supplier_cusip.index)
supplier_isin = supplier.loc[(supplier['SOURCE_isin'].notna()) & (supplier['TARGET_isin'].notna())]
supplier = supplier.drop(supplier_isin.index)
supplier_ticker = supplier.loc[(supplier['SOURCE_name'].notna()) & (supplier['TARGET_name'].notna())]
supplier = supplier.drop(supplier_ticker.index)

customer_cusip = customer.loc[(customer['SOURCE_cusip'].notna()) & (customer['TARGET_cusip'].notna())]
customer = customer.drop(customer_cusip.index)
customer_isin = customer.loc[(customer['SOURCE_isin'].notna()) & (customer['TARGET_isin'].notna())]
customer = customer.drop(customer_isin.index)
customer_ticker = customer.loc[(customer['SOURCE_name'].notna()) & (customer['TARGET_name'].notna())]
customer = customer.drop(customer_ticker.index)

cik_compustat = pd.read_csv('./supply_chain/cik_compustat.csv')
company_dir_names = pd.read_csv('./supply_chain/wrds_company_dir_names.csv')

supplier_cusip = duckdb.query("""
SELECT a.*, b.gvkey, b.CUSIPH, b.cik, b.FNDATE, b.LNDATE
FROM supplier_cusip as a
LEFT JOIN cik_compustat as b
ON a.SOURCE_cusip = b.CUSIPH and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
supplier_cusip = supplier_cusip.drop(['CUSIPH', 'FNDATE', 'LNDATE'], axis=1)
supplier_cusip = supplier_cusip.rename(columns={'gvkey': 'SOURCE_gvkey',
                                                'cik': 'SOURCE_cik'})
supplier_cusip = duckdb.query("""
SELECT a.*, b.gvkey, b.CUSIPH, b.cik, b.FNDATE, b.LNDATE
FROM supplier_cusip as a
LEFT JOIN cik_compustat as b
ON a.TARGET_cusip = b.CUSIPH and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
supplier_cusip = supplier_cusip.drop(['CUSIPH', 'FNDATE', 'LNDATE'], axis=1)
supplier_cusip = supplier_cusip.rename(columns={'gvkey': 'TARGET_gvkey',
                                                'cik': 'TARGET_cik'})

supplier_isin = duckdb.query("""
SELECT a.*, b.isin, b.cikcode
FROM supplier_isin as a
LEFT JOIN company_dir_names as b
ON a.SOURCE_isin = b.isin
""").df()
supplier_isin = duckdb.query("""
SELECT a.*, b.gvkey, b.cik, b.FNDATE, b.LNDATE
FROM supplier_isin as a
LEFT JOIN cik_compustat as b
ON a.cikcode = b.cik and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
supplier_isin = supplier_isin.drop(['isin', 'cikcode', 'FNDATE', 'LNDATE'], axis=1)
supplier_isin = supplier_isin.rename(columns={'gvkey': 'SOURCE_gvkey',
                                              'cik': 'SOURCE_cik'})

supplier_isin = duckdb.query("""
SELECT a.*, b.isin, b.cikcode
FROM supplier_isin as a
LEFT JOIN company_dir_names as b
ON a.TARGET_isin = b.isin
""").df()
supplier_isin = duckdb.query("""
SELECT a.*, b.gvkey, b.cik, b.FNDATE, b.LNDATE
FROM supplier_isin as a
LEFT JOIN cik_compustat as b
ON a.cikcode = b.cik and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
supplier_isin = supplier_isin.drop(['isin', 'cikcode', 'FNDATE', 'LNDATE'], axis=1)
supplier_isin = supplier_isin.rename(columns={'gvkey': 'TARGET_gvkey',
                                              'cik': 'TARGET_cik'})

customer_cusip = duckdb.query("""
SELECT a.*, b.gvkey, b.CUSIPH, b.cik, b.FNDATE, b.LNDATE
FROM customer_cusip as a
LEFT JOIN cik_compustat as b
ON a.SOURCE_cusip = b.CUSIPH and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
customer_cusip = customer_cusip.drop(['CUSIPH', 'FNDATE', 'LNDATE'], axis=1)
customer_cusip = customer_cusip.rename(columns={'gvkey': 'SOURCE_gvkey',
                                                'cik': 'SOURCE_cik'})
customer_cusip = duckdb.query("""
SELECT a.*, b.gvkey, b.CUSIPH, b.cik, b.FNDATE, b.LNDATE
FROM customer_cusip as a
LEFT JOIN cik_compustat as b
ON a.TARGET_cusip = b.CUSIPH and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
customer_cusip = customer_cusip.drop(['CUSIPH', 'FNDATE', 'LNDATE'], axis=1)
customer_cusip = customer_cusip.rename(columns={'gvkey': 'TARGET_gvkey',
                                                'cik': 'TARGET_cik'})

customer_isin = duckdb.query("""
SELECT a.*, b.isin, b.cikcode
FROM customer_isin as a
LEFT JOIN company_dir_names as b
ON a.SOURCE_isin = b.isin
""").df()
customer_isin = duckdb.query("""
SELECT a.*, b.gvkey, b.cik, b.FNDATE, b.LNDATE
FROM customer_isin as a
LEFT JOIN cik_compustat as b
ON a.cikcode = b.cik and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
customer_isin = customer_isin.drop(['isin', 'cikcode', 'FNDATE', 'LNDATE'], axis=1)
customer_isin = customer_isin.rename(columns={'gvkey': 'SOURCE_gvkey',
                                              'cik': 'SOURCE_cik'})

customer_isin = duckdb.query("""
SELECT a.*, b.isin, b.cikcode
FROM customer_isin as a
LEFT JOIN company_dir_names as b
ON a.TARGET_isin = b.isin
""").df()
customer_isin = duckdb.query("""
SELECT a.*, b.gvkey, b.cik, b.FNDATE, b.LNDATE
FROM customer_isin as a
LEFT JOIN cik_compustat as b
ON a.cikcode = b.cik and a.date <= b.LNDATE and a.date >= b.FNDATE
""").df()
customer_isin = customer_isin.drop(['isin', 'cikcode', 'FNDATE', 'LNDATE'], axis=1)
customer_isin = customer_isin.rename(columns={'gvkey': 'TARGET_gvkey',
                                              'cik': 'TARGET_cik'})

cik_compustat = pd.read_csv('./supply_chain/cik_compustat.csv')
cik_compustat['coname'] = cik_compustat['coname'].astype(str).apply(lambda x: cleanco.basename(x.lower()))

pol_covid = pd.read_stata('./risk_exposure/08_all_scores/pol_covid.dta')
cc = pd.read_csv('./risk_exposure/08_all_scores/cc_scores.csv')

linktable_name = cik_compustat.loc[cik_compustat['gvkey'].isin(pd.concat([pol_covid['gvkey'], cc['gvkey']]).drop_duplicates().dropna())].drop_duplicates()


def fuzzy_match(name, names):
    names = pd.DataFrame(names)
    names['name_start'] = str(name)[:1]
    names['name_start'].astype(str)
    names['start'] = names['name'].apply(lambda x: str(x)[:1])
    names['start'] = names['start'].astype(str)
    names_same_startswith = names.loc[names['start'] == names['name_start'], 'name']
    # print(process.extractBests(name, names_same_startswith, scorer=fuzz.token_set_ratio, score_cutoff=90))
    return process.extractBests(name, names_same_startswith, scorer=fuzz.token_set_ratio, score_cutoff=90)


names = pd.concat([supplier_ticker['SOURCE_name'], supplier_ticker['TARGET_name'], customer_ticker['SOURCE_name'], customer_ticker['TARGET_name']]).drop_duplicates().dropna()
names = pd.DataFrame(names)
names = names.rename(columns={0: 'name'})
names['name'] = names['name'].astype(str)
name = linktable_name['coname'].astype(str)
tqdm.pandas()
linktable_name['coname'] = linktable_name['coname'].progress_apply(lambda x: fuzzy_match(x, names))

linktable_name.to_csv('./linktables/cik_compustat_name.csv')


import numpy as np


def extract_best_match(name):
    try:
        return name[0][0]
    except IndexError:
        return np.nan


linktable_name['coname'] = linktable_name['coname'].progress_apply(lambda x: extract_best_match(x))
