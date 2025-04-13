import pandas as pd
import numpy as np
import datetime
import pickle


class Metric_Calculators:
    def __init__(self):
        """path1 = 'app/data/category_wise_isin.pkl'
        path2 = 'app/data/isin_to_fund_name.pkl'
        path0 = 'ISINs_with_ids.csv'
        df = pd.read_csv(path0)
        res1 = df.groupby('sub_category')['isin'].agg(list).to_dict()
        res2 = df[['Scheme Name', 'isin']].set_index('isin').to_dict()
        
        with open(path1, 'wb') as f:
            pickle.dump(res1 ,f)

        with open(path2, 'wb') as f:
            pickle.dump(res2 ,f)"""
        pass
        

#Converts epoch time to datetime format
    def epoch_to_date(self, epoch):

        return datetime.datetime.strptime(datetime.datetime.fromtimestamp(epoch).strftime('%Y-%m-%d'),  "%Y-%m-%d")
    
    def all_added_categories(self):
        path = 'app/data/category_wise_isin.pkl'     
        with open(path, 'rb') as f:
            mapping = pickle.load(f)

        categories = [cat for cat in mapping.keys()]
        return categories
    
#gives all the isins of a particular category for eg: for largecap   
    def category_to_isin(self, category = 'Largecap - Index'):
        path = 'app/data/category_wise_isin.pkl'
        with open(path, 'rb') as f:
            mapping = pickle.load(f)

        try:
            return mapping[category]
        
        except:
            return None 
        
    
#gives the fund name give an isin
    def isin_to_fund_name(self, isin = 'INF109K01PI0'):
        path = 'app/data/isin_to_fund_name.pkl'
        with open(path, 'rb') as f:
            mapping = pickle.load(f)


        return mapping['Scheme Name'][isin]
        

class DataPreprocessor:
    def __init__(self, data):
        self.nav = data
        self.start_dt = self.nav.dates.iloc[0]
        self.end_dt = self.nav.dates.iloc[-1]

    def preprocess_dates(self):

        dates = pd.DataFrame()
        dates["dates"] = list(
            set(pd.date_range(start=self.start_dt, end=self.end_dt, freq="B")))
        dates.sort_values(
            by="dates", inplace=True
        )  # business/trading dates in this range
        dates.dates = pd.to_datetime(dates.dates)

        self.nav.dates = pd.to_datetime(
            self.nav.dates)
        self.nav.sort_values(by="dates", inplace=True)
        self.nav = pd.merge(dates, self.nav, on="dates", how="left")
        self.nav.sort_values(by="dates", inplace=True)

    def preprocess_missing(self):
        for col in self.nav.columns:
            if col != "dates":
                self.nav[col] = self.nav[col].ffill().add(
                    self.nav[col].bfill()).div(2)

    def data_preprocessing(self):
        self.preprocess_dates()
        self.preprocess_missing()

        return self.nav

    def preprocess_nav(self):
        return self.data_preprocessing() 


M = Metric_Calculators()