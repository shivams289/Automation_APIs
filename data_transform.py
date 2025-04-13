import pandas as pd
import numpy as np
import datetime as dt
isin_map = pd.read_csv('ISINs_with_ids.csv')
isin_map = isin_map[['Scheme Name', 'isin']].set_index('Scheme Name')
isin_map = isin_map.to_dict()
# print(isin_map)
# print(scheme_names)

def fund_ts_creator(nav, schemes):
    final = pd.DataFrame()
    for fund in schemes:
        new = pd.DataFrame(columns=["dates", "scheme_id", "nav"])
        new[["dates", "nav"]] = nav[[
            "dates", fund]].dropna(axis=0).values
        new["scheme_id"] = fund
        final = pd.concat([final, new], axis=0, ignore_index=True)

    final["id"] = np.arange(1, len(final) + 1)
    final["created_dt"] = dt.date.today()
    final["created_by"] = 1
    final["updated_dt"] = dt.date.today()
    final["updated_by"] = 1

    final.dates = (final.dates - dt.datetime(1970, 1, 1)
                    ).dt.total_seconds()
    final.created_dt = pd.to_datetime(
        final.created_dt)
    final.created_dt = (
        final.created_dt - dt.datetime(1970, 1, 1)
    ).dt.total_seconds()

    final.updated_dt = pd.to_datetime(
        final.updated_dt)
    final.updated_dt = (
        final.updated_dt - dt.datetime(1970, 1, 1)
    ).dt.total_seconds()

    return final
path = 'ind.xlsx'
nav = pd.read_excel(path)
cols = []
for col in nav.columns:
    if col!= 'dates' and col in isin_map['isin'].keys():
        nav.rename(columns={col:isin_map['isin'][col]}, inplace=True)
        cols.append(isin_map['isin'][col])
# print(cols)
final = fund_ts_creator(nav=nav, schemes=cols)
print(final.head())
final.to_csv('ind_res.csv', index=False)