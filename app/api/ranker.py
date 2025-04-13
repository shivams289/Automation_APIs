from flask import Blueprint, request, jsonify
from datetime import datetime
from sqlalchemy import distinct
from app.model import PerformanceHistory
from app.extensions import db
from app.portfolio_computation import Metric_Calculators
from app.momentum import MomentumRotationNorm
from app.weighted_avg_ranker import WeightedAvgOutperformanceRanker
import pandas as pd

metrics = Metric_Calculators()
Perf_table = PerformanceHistory

top_funds_momentum_bp = Blueprint('top_funds1', __name__)
top_funds_outperf_bp = Blueprint('top_funds2', __name__)

def get_fund_data_by_category(category, add_index = False):
    isin_list = metrics.category_to_isin(category=category)
    if add_index:
        isin_list += metrics.category_to_isin(category=add_index)

    if not isin_list:
        return None, 'ISIN data not available in db'

    all_funds = db.session.query(Perf_table.date, Perf_table.scheme_id, Perf_table.nav)\
                              .distinct().filter(Perf_table.scheme_id.in_(isin_list)).all()

    if not all_funds:
        return None, 'ISIN data not available in db'

    data = pd.DataFrame(all_funds)
    data.date = data.date.apply(metrics.epoch_to_date)
    data.drop_duplicates(subset=['date', 'scheme_id'], keep='first', inplace=True)
    data = data.pivot(index='date', columns='scheme_id', values='nav')
    data.reset_index(inplace=True)
    data.date = pd.to_datetime(data.date).dt.date
    data.sort_values(by='date', inplace=True)

    return data, None

def comparable_index(category):
    indices =['Largecap - Index', 'Midcap - Index', 'Smallcap - Index', 'Value - Index']
    if category == 'Midcap - Active':
        return indices[1]

    elif category == 'Smallcap - Active':
        return indices[2]

    elif category == 'Value - Active':
        return indices[3]

    else:
        return indices[0]




def calculate_momentum_ranks(data):
    momentum = MomentumRotationNorm(data=data)
    final = momentum.period_end_top_n_signal()
    isin_with_scores = final[1]
    return {metrics.isin_to_fund_name(isin=isin): score for isin, score in isin_with_scores.items()}

def calculate_outperformance(data, category, index):
    out = WeightedAvgOutperformanceRanker(data = data, category= category, index= index)
    ranked = out.Ranker(lookback_years=2, rolling_lag_days=3*22)
    ranked = {metrics.isin_to_fund_name(isin=isin): score for isin, score in ranked.items()}

    return ranked

@top_funds_momentum_bp.route('/top-funds/momentum', methods=['GET'])
def rank_funds():
    category = request.args.get('category')
    response_data = []

    if category:
        data, error = get_fund_data_by_category(category)
        if error:
            return jsonify({'data': response_data, 'message': error}), 404
        
        fund_with_scores = calculate_momentum_ranks(data)
        response_data = {'category': category, 'MomentumRanked': fund_with_scores}
    else:
        categories = metrics.all_added_categories()
        # categories = ['Largecap - Active', 'Smallcap - Active']
        for category in categories:
            data, error = get_fund_data_by_category(category)
            if error:
                response_data.append({'category': category, 'error': error})
                continue
            
            fund_with_scores = calculate_momentum_ranks(data)
            response_data.append({'category': category, 'MomentumRanked': fund_with_scores})

    return jsonify({'data': response_data, 'message': 'Fetched successfully'}), 200


@top_funds_outperf_bp.route('/top-funds/outperf', methods=['GET'])
def rank_funds():
    category = request.args.get('category')
    response_data = []
    categories = ['Balanced Advantage', 'Conservative', 'Equity Savings', 'Large & Mid', 'Largecap - Active', 'Midcap - Active', 'Multi - Asset', 'Smallcap - Active', 'Value']
    

    if category:
        if category in categories:
            ix = comparable_index(category)
            data, error = get_fund_data_by_category(category, add_index=ix)
            if error:
                return jsonify({'data': response_data, 'message': error}), 404
            
            fund_with_scores = calculate_outperformance(data=data, category=category, index=ix)
            response_data = {'category': category, 'Outperformace_Ranked': fund_with_scores}

        else:
            jsonify({'data': response_data, 'message': 'Ranker Not Available For This Category'}), 404

    else:
        for category in categories:
            ix = comparable_index(category)
            data, error = get_fund_data_by_category(category, add_index=ix)
            if error:
                response_data.append({'category': category, 'error': error})
                continue
            
            fund_with_scores = calculate_outperformance(data=data, category=category, index=ix)
            response_data.append({'category': category, 'Outperformace_Ranked': fund_with_scores})

    return jsonify({'data': response_data, 'message': 'Fetched successfully'}), 200
