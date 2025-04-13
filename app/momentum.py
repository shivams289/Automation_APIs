import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import datetime as dt
from dateutil.relativedelta import relativedelta
from app.portfolio_computation import DataPreprocessor

class MomentumRotationNorm:
    def __init__(self, data):
        self.df = data
        self.df.rename(columns = {'date':'dates'}, inplace=True)
        self.df = DataPreprocessor(data=self.df).preprocess_nav()
        self.cols = list(set(self.df.columns) - set(['dates']))

    def annual_volatility(self):
        daily_return_df = self.df[self.cols].pct_change(fill_method = None)
        annual_volatility = daily_return_df[self.cols].rolling(window = 261).std()*np.sqrt(261)
        annual_volatility['dates'] = self.df.dates

        return annual_volatility

    def returns(self, freq = 'Q'):
        if freq == 'Q':
            month_ends = [4,7,10,1]
        elif freq == 'SA':
            month_ends = [5,11]

        else:
            month_ends = [i for i in range(1,13)]

        df = self.df.copy(deep=True)
        df['month'] = df.dates.dt.month
        df['year'] = df.dates.dt.year
        month_df = df.groupby(['year', 'month']).head(1).reset_index(drop=True)
        month_df = month_df[month_df.month.isin(month_ends)].reset_index(drop=True)
        
        return_12m = month_df[self.cols].pct_change(periods=len(month_ends), fill_method = None)
        return_6m = month_df[self.cols].pct_change(periods=len(month_ends)//2, fill_method = None)
        return_12m['dates'] = month_df.dates
        return_6m['dates'] = month_df.dates
        self.signal_dates = month_df.dates

        return return_12m, return_6m

    def momentum_scores(self):
        annual_volatility = self.annual_volatility()
        return_12m, return_6m = self.returns()
        annual_volatility = annual_volatility[annual_volatility.dates.isin(return_12m.dates)].reset_index(drop=True)
        momentum_scores_12m = return_12m[self.cols].div(annual_volatility[self.cols], axis=0)
        momentum_scores_6m = return_6m[self.cols].div(annual_volatility[self.cols], axis=0)

        return momentum_scores_12m, momentum_scores_6m

    def calculate_z_score(self):
        momentum_scores_12m, momentum_scores_6m = self.momentum_scores()
        if len(momentum_scores_12m.columns) == 1:
            z_scores_12m = (momentum_scores_12m.sub(momentum_scores_12m.mean(axis=1).fillna(1), axis=0)).div(momentum_scores_12m.std(axis=1).fillna(1), axis=0)
            z_scores_6m = (momentum_scores_6m.sub(momentum_scores_6m.mean(axis=1).fillna(1), axis=0)).div(momentum_scores_6m.std(axis=1).fillna(1), axis=0)

        else:
            z_scores_12m = (momentum_scores_12m.sub(momentum_scores_12m.mean(axis=1), axis=0)).div(momentum_scores_12m.std(axis=1), axis=0)
            z_scores_6m = (momentum_scores_6m.sub(momentum_scores_6m.mean(axis=1), axis=0)).div(momentum_scores_6m.std(axis=1), axis=0)

        wtd_avg_z_score = (z_scores_6m + z_scores_12m)/2

        return wtd_avg_z_score
    def normalized_z_score(self):
        wtd_avg_z_score = self.calculate_z_score()
        norm_z_score = wtd_avg_z_score.map(lambda x: (1+x) if x>0 else (1-x)**(-1))

        return norm_z_score

    def period_end_top_n_signal(self, n=4):
        norm_z_score = self.normalized_z_score()
        def top_n_columns(row, n=6):
            n = len(row.dropna())
            final = row.nlargest(n).to_dict()
            return final

        # Apply the function row-wise
        top_columns = norm_z_score.apply(lambda row: top_n_columns(row,n), axis=1)
        res = {}
        i = 0
        for dates in self.signal_dates.dt.date:
            res[dates] = top_columns[i]
            i+=1
        res = list(res.items())

        return res[-1]
    

class MomentumRotation:
    def __init__(self, path = '/content/drive/MyDrive/Low_Volatility_Indices_Nov23_result.xlsx'):
        self.df = pd.read_excel(path)
        considered_indices = ['dates','NIFTY 50 - TRI','Nifty Midcap 150 - TRI',
                     'Nifty Smallcap 250 - TRI', 'NIFTY NEXT 50 - TRI', 'DEBT_CONSTANT_MATURITY',
                             'GOLD']
        self.df = self.df[considered_indices]
        self.cols = list(set(self.df.columns) - set(['dates']))

    def annual_volatility(self):
        daily_return_df = self.df[self.cols].pct_change()
        annual_volatility = daily_return_df[self.cols].rolling(261).std()*np.sqrt(261)
        annual_volatility['dates'] = self.df.dates

        return annual_volatility

    def returns(self, freq = 'Q'):
        if freq == 'Q':
            month_ends = [4,7,10,1]
        elif freq == 'SA':
            month_ends = [5,11]

        else:
            month_ends = [i for i in range(1,13)]

        df = self.df.copy(deep=True)
        df['month'] = df.dates.dt.month
        df['year'] = df.dates.dt.year
        month_df = df.groupby(['year', 'month']).head(1).reset_index(drop=True)
        month_df = month_df[month_df.month.isin(month_ends)].reset_index(drop=True)
        return_12m = month_df[self.cols].pct_change(periods=len(month_ends))
        return_6m = month_df[self.cols].pct_change(periods=len(month_ends)//2)
        return_3m = month_df[self.cols].pct_change(periods=len(month_ends)//4)
        return_12m['dates'] = month_df.dates
        return_6m['dates'] = month_df.dates
        return_3m['dates'] = month_df.dates
        self.signal_dates = month_df.dates
#         print(return_12m.head(1))

        return return_12m, return_6m

    def relative_momentum_scores(self):
        annual_volatility = self.annual_volatility()
        return_12m, return_6m = self.returns()
        annual_volatility = annual_volatility[annual_volatility.dates.isin(return_12m.dates)].reset_index(drop=True)
#         momentum_scores_12m = return_12m[self.cols].div(annual_volatility[self.cols], axis=0)
#         momentum_scores_6m = return_6m[self.cols].div(annual_volatility[self.cols], axis=0)
        momentum_scores_12m = return_12m[self.cols]
        momentum_scores_6m = return_6m[self.cols]

        return momentum_scores_12m, momentum_scores_6m

    def calculate_z_score(self):
        momentum_scores_12m, momentum_scores_6m = self.relative_momentum_scores()

        z_scores_12m = (momentum_scores_12m.sub(momentum_scores_12m.mean(axis=1), axis=0)).div(momentum_scores_12m.std(axis=1), axis=0)
        z_scores_6m = (momentum_scores_6m.sub(momentum_scores_6m.mean(axis=1), axis=0)).div(momentum_scores_6m.std(axis=1), axis=0)
        wtd_avg_z_score = z_scores_6m

        return wtd_avg_z_score
    def normalized_z_score(self):
        wtd_avg_z_score = self.calculate_z_score()
        norm_z_score = wtd_avg_z_score.applymap(lambda x: (1+x) if x>0 else (1-x)**(-1))

        return norm_z_score

    def absolute_momentum_scores(self):
      return_12m, return_6m = self.returns()
      final = (return_12m[self.cols] + return_6m[self.cols])/2

      return final

    def period_end_top_n_signal(self, n=4):
        norm_z_score = self.normalized_z_score()
        absolute_momentum = self.absolute_momentum_scores()
        absolute_momentum['dates'] = self.signal_dates.dt.date
        absolute_momentum = absolute_momentum.set_index('dates', drop=True).to_dict('index')
        # print(f"absolute momentum {absolute_momentum}")
        # print(norm_z_score)
        def top_n_columns(row, n=6):
            final = row.nlargest(n).to_dict()
            return final

        # Apply the function row-wise
        top_columns = norm_z_score.apply(lambda row: top_n_columns(row,n), axis=1)
        print(f"top columns : {len(top_columns)}, absolute momentum : {len(absolute_momentum)}")
        res = {}
        i = 0
        for dates in self.signal_dates.dt.date:
            to_take = {}
            for el in top_columns[i].keys():
                if absolute_momentum[dates][el]>0:
                    to_take[el] = top_columns[i][el]

            to_take = dict(sorted(to_take.items(), key = lambda key:key[1], reverse = True))
            res[dates] = to_take
            i+=1

        return res




