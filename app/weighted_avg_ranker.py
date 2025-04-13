import pandas as pd
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
from app.portfolio_computation import Metric_Calculators
# Cat_index can be 'MIDCAP', 'SMALLCAP', 'LARGECAP'

metrics = Metric_Calculators()
class WeightedAvgOutperformanceRanker:
    def __init__(self, data, category, index ):
        index_isin = metrics.category_to_isin(category=index)
        self.end_dt = data.date.iat[-1]
        rel = relativedelta(years=3)
        self.start_dt = (self.end_dt-rel)

        nav = self.data_preprocessing(data)
        self.nav = nav
        print('shape of nav before dropping', self.nav.shape)
        self.nav.dropna(inplace=True)
        print('shape of nav before dropping', self.nav.shape)
        self.category_index = index_isin[0]

        print("..............Data Loaded.............")
        print(f"ANALYZING {category} vs {index} INDEX")

    # start, end date format id mm/dd/yy
    def preprocess_dates(self, nav):
        dates = pd.date_range(start=self.start_dt,
                              end=self.end_dt, freq="B").date
        nav.date = pd.to_datetime(
            nav.date).dt.date

        merged_nav = pd.DataFrame({"date": dates})
        merged_nav = merged_nav.merge(nav, on="date", how="left")
        merged_nav.sort_values(by="date", inplace=True)

        return merged_nav

    def preprocess_missing(self, nav):
        for col in nav.columns:
            if col != "date":
                nav[col] = nav[col].bfill()

        return nav

    def data_preprocessing(self, nav):
        nav = self.preprocess_dates(nav=nav)
        nav = self.preprocess_missing(nav=nav)
        print("........Data Preprocessed...........")

        return nav


    def generate_rolling_return(self, data, lookback_years=3, lag=22):
        rel = relativedelta(months=12*lookback_years)

        data = data[data.date <= self.end_dt]

        if data.date.iloc[0] <= (self.end_dt - rel):
            data = data[data.date >= (self.end_dt - rel)]

        else:
            data = data.loc[(data.date <= self.end_dt) & (data.date >= (self.end_dt-rel))]

        data.reset_index(drop=True, inplace=True)
        col_names = data.columns[~data.columns.str.contains('date')]
        if lag <= 261:
            ret = (data[col_names].shift(-lag)/data[col_names]) - 1
        else:
            ret = (data[col_names].shift(-lag)/data[col_names])**(261/lag) - 1
        ret['date'] = data.date.shift(-lag)
        ret.dropna(inplace=True)
        print(
            f"...........{lag/261} Rolling Return years is generated...........")

        return ret

    def generate_returns(self, data, lookback_years=3, lag=22):
        rel = relativedelta(months=12*lookback_years)
        data = data[data.date <= self.end_dt]

        if data.date.iloc[0] <= (self.end_dt - rel):
            data = data[data.date >= (self.end_dt - rel)]

        else:
            data = data.loc[(data.date <= self.end_dt) & (data.date >= (self.end_dt-rel))]

        data.reset_index(drop=True, inplace=True)
        # data.dropna(axis=1, inplace=True)
        col_names = data.columns[~data.columns.str.contains('date')]
        if lag <= 261:
            ret = (data[col_names].shift(-lag)/data[col_names]) - 1
        else:
            ret = (data[col_names].shift(-lag)/data[col_names])**(261/lag) - 1
        ret['date'] = data.date.shift(-lag)
        ret.dropna(inplace=True)
        print(
            f"...........{lag/261} Rolling Return years is generated for correlation...........")

        return ret

    def Ranker(self, lookback_years=3, rolling_lag_days=22):
        category_index = self.category_index
        data = self.generate_rolling_return(data=self.nav, lookback_years=lookback_years, lag=rolling_lag_days)
        col_names = data.columns[~data.columns.str.contains('date')]
        smallcap_names = col_names
    
        ret_data_1d = self.generate_returns(data=self.nav, lookback_years=lookback_years, lag=1)
        ret_data_1m = self.generate_returns(data=self.nav, lookback_years=lookback_years, lag=22)
        ret_data_3m = self.generate_returns(data=self.nav, lookback_years=lookback_years, lag=66)

        def calculate_metrics(data):
            res = {"fund_name": [], "percent_rolling_avg": [], "beated_by_percent_avg": [], "lost_by_percent_avg": [], "perc_times_beated": [
            ], "beated_by_percent_max": [], "beated_by_percent_min": [], "lost_by_percent_min": [], "lost_by_percent_max": [], "Daily_Return_Corr": [], "Monthly_Return_Corr": [], "Quaterly_Return_Corr": []}
            for x in list(set(col_names)-set([category_index])):
                res['fund_name'].append(x)
                rolling_avg = data[x].mean()*100

                res['Daily_Return_Corr'].append(ret_data_1d[x].corr(
                    ret_data_1d[self.category_index]))
                res['Monthly_Return_Corr'].append(ret_data_1m[x].corr(
                    ret_data_1m[self.category_index]))
                res['Quaterly_Return_Corr'].append(ret_data_3m[x].corr(
                    ret_data_3m[self.category_index]))

                beat_perc = max(0, (data.loc[(data[x] >= data[category_index]), x] - data.loc[(
                    data[x] >= data[category_index]), category_index]).mean()*100)
                lost_perc = min(0, (data.loc[(data[x] < data[category_index]), x] - data.loc[(
                    data[x] < data[category_index]), category_index]).mean()*100)

                beat_perc_max = max(0, (data.loc[(data[x] >= data[category_index]), x] - data.loc[(
                    data[x] >= data[category_index]), category_index]).max()*100)
                beat_perc_min = max(0, (data.loc[(data[x] >= data[category_index]), x] - data.loc[(
                    data[x] >= data[category_index]), category_index]).min()*100)
                lost_perc_min = min(0, (data.loc[(data[x] < data[category_index]), x] - data.loc[(
                    data[x] < data[category_index]), category_index]).min()*100)
                lost_perc_max = min(0, (data.loc[(data[x] < data[category_index]), x] - data.loc[(
                    data[x] < data[category_index]), category_index]).max()*100)

                pct_times_beated = max(0, data.loc[(
                    data[x] >= data[category_index])].shape[0]/data.shape[0]*100)

                res['percent_rolling_avg'].append(rolling_avg)
                res['beated_by_percent_avg'].append(beat_perc)
                res['lost_by_percent_avg'].append(lost_perc)
                res['beated_by_percent_max'].append(beat_perc_max)
                res['beated_by_percent_min'].append(beat_perc_min)
                res['lost_by_percent_min'].append(lost_perc_min)
                res['lost_by_percent_max'].append(lost_perc_max)

                wtd_avg_out = pct_times_beated*beat_perc + \
                    (100-pct_times_beated)*lost_perc

                res['perc_times_beated'].append(
                    pct_times_beated)

            res = pd.DataFrame(res)
            res['Wtd_avg_outperformance'] = res['perc_times_beated']*res['beated_by_percent_avg'] + \
                (100-res['perc_times_beated'])*res['lost_by_percent_avg']

            return res.fillna(0)

        p1 = int(0.5*len(data))  # part 1 of data: 50%
        res1 = calculate_metrics(data.iloc[:p1, :])
        res2 = calculate_metrics(data.iloc[p1:, :])
        res = 0.4*res1.set_index('fund_name')+(0.6*res2.set_index('fund_name'))
        res.sort_values(
            by=['Wtd_avg_outperformance', 'perc_times_beated'], ascending=False, inplace=True)
        res['return_rank'] = [x for x in range(1, len(res)+1)]
        rel = relativedelta(months=12*lookback_years)

        self.nav = self.nav.loc[(self.nav.date >= (self.end_dt - rel)) & (
            self.nav.date <= self.end_dt)]
        dd = (self.nav[smallcap_names].cummax(
        ) - self.nav[smallcap_names]).div(self.nav[smallcap_names].cummax())

        maxdd = pd.DataFrame((self.nav[smallcap_names].cummax() - self.nav[smallcap_names]).div(self.nav[smallcap_names].cummax()).max()*100, columns=[
            'MaxDrawdown']).reset_index()
        maxdd.rename(columns={'index': 'fund_name'}, inplace=True)
        resdd = {"fund_name": [], "dd_less_by_percent_avg": [], "dd_greater_by_percent_avg": [
        ], "perc_times_dd_less": [], 'dd_greater_max': [], 'dd_less_min': []}
        for x in list(set(smallcap_names)-set([category_index])):
            resdd['fund_name'].append(x)
            beat_perc = (dd.loc[(dd[x] <= dd[category_index]), x] -
                         dd.loc[(dd[x] <= dd[category_index]), category_index]).mean()*100
            lost_perc = (dd.loc[(dd[x] > dd[category_index]), x] -
                         dd.loc[(dd[x] > dd[category_index]), category_index]).mean()*100

            dd_less_min = (dd.loc[(dd[x] <= dd[category_index]), x] -
                           dd.loc[(dd[x] <= dd[category_index]), category_index]).min()*100
            dd_greater_max = (dd.loc[(dd[x] > dd[category_index]), x] -
                              dd.loc[(dd[x] > dd[category_index]), category_index]).max()*100

            resdd['dd_less_by_percent_avg'].append(beat_perc)
            resdd['dd_greater_by_percent_avg'].append(lost_perc)
            resdd['perc_times_dd_less'].append(
                dd.loc[(dd[x] <= dd[category_index])].shape[0]/dd.shape[0]*100)

            resdd['dd_greater_max'].append(dd_greater_max)
            resdd['dd_less_min'].append(dd_less_min)

        resdd = pd.DataFrame(resdd)
        res = pd.merge(res, resdd, on='fund_name', how='left')
        res = pd.merge(res, maxdd, on='fund_name', how='left')

        res = dict(zip(res.fund_name, res.return_rank))

        return res
