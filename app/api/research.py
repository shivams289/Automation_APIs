from flask import Blueprint, request, jsonify
from dateutil.relativedelta import relativedelta
from datetime import datetime
import datetime
from sqlalchemy import func
from app.model import PerformanceHistory
from app.extensions import db
from app.portfolio_computation import Metric_Calculators
metrics = Metric_Calculators()
Perf_table = PerformanceHistory

research_bp = Blueprint('research', __name__)
db_conn_bp = Blueprint('db_conn', __name__)
returns_bp = Blueprint('returns', __name__)

#This Api is designed to check the health of the server, if it is working correctly or not
@research_bp.route('/health', methods=['GET'])
def health_check():
    response = {
        'status': 'OK',
        'message': 'The server is healthy!'
    }
    return jsonify(response), 200

#This api is designed to check the connection of db and server if the connection libraries are working fine or not
@db_conn_bp.route('/db_conn', methods=['GET'])
def db_connection_check():
    fund = Perf_table.query.filter_by(scheme_id='INF204K01562').first()
    
    # Check if the fund exists
    
    if fund:
        # Construct fund data dictionary
        fund_data = {'date': fund.date, 'nav': fund.nav}
        # Return user fund data as JSON response
        return jsonify(fund_data)
    else:
        # Return a message indicating that the fund was not found
        return jsonify({'message': 'fund not found'}), 404
    


def get_nav_for_closest_date(isin, target_date):
    target_epoch = int(target_date.timestamp())
    nav_entry = db.session.query(Perf_table.nav, Perf_table.date).filter(
        Perf_table.scheme_id == isin,
        Perf_table.date <= target_epoch
    ).order_by(Perf_table.date.desc()).first()
    
    if not nav_entry:
        nav_entry = 'fund_unavailable'
        return nav_entry
    
    return nav_entry.nav

#This api is the first useful api and will return cagr returns of multiple periods  
@returns_bp.route('/returns', methods=['GET'])
def returns():
    isin = request.args.get('ISIN') #will fetch the ISIN if it's provided
    response_data = []
    if isin:
        fund = db.session.query(Perf_table).filter(Perf_table.scheme_id == isin).order_by(Perf_table.date.desc()).first()
        if fund:
            latest_nav = fund.nav
            latest_date = metrics.epoch_to_date(fund.date)

            print("----------------------------------")
            print(f"Nav For {isin} on date {latest_date} is {latest_nav}")

            three_years_ago = latest_date - relativedelta(years=3)
            five_years_ago = latest_date - relativedelta(years=5)
            y3_nav = get_nav_for_closest_date(isin, three_years_ago)
            y5_nav = get_nav_for_closest_date(isin, five_years_ago)

            if y3_nav == 'fund_unavailable':
                y3_cagr, y5_cagr = 'NA', 'NA'
            elif y5_nav == 'fund_unavailable':
                y3_cagr, y5_cagr = round((latest_nav/y3_nav)**(1/3)-1, 4), 'NA'
            else:
                y3_cagr, y5_cagr = round((latest_nav/y3_nav)**(1/3)-1, 4),  round((latest_nav/y5_nav)**(1/5)-1, 4)

            response_data = {'isin':fund.scheme_id, 'latest_nav_date':latest_date,'3y_cagr':y3_cagr, '5y_cagr':y5_cagr}
            return jsonify({'data':response_data, 'message':'fetched successfully'}), 200
        
        else:
            return jsonify({'data':response_data, 'message':'isin was not found'}), 404


    else:
        subquery = db.session.query(Perf_table.scheme_id, func.max(Perf_table.date).label('last_date')).group_by(Perf_table.scheme_id).subquery()

        query = db.session.query(Perf_table.scheme_id,Perf_table.nav, Perf_table.date).join(subquery,(Perf_table.scheme_id == subquery.c.scheme_id) & (Perf_table.date == subquery.c.last_date))

        result = query.all()
        for row in result:
            isin = row.scheme_id
            latest_nav = row.nav
            latest_date = metrics.epoch_to_date(row.date)

            three_years_ago = latest_date - relativedelta(years=3)
            five_years_ago = latest_date - relativedelta(years=5)

            y3_nav = get_nav_for_closest_date(isin, three_years_ago)
            y5_nav = get_nav_for_closest_date(isin, five_years_ago)
            if y3_nav == 'fund_unavailable':
                y3_cagr, y5_cagr = 'NA', 'NA'
            elif y5_nav == 'fund_unavailable':
                y3_cagr, y5_cagr = round((latest_nav/y3_nav)**(1/3)-1, 4), 'NA'
            else:
                y3_cagr, y5_cagr = round((latest_nav/y3_nav)**(1/3)-1, 4),  round((latest_nav/y5_nav)**(1/5)-1, 4)


            response_data.append(
                { "isin": isin, 
                 "latest_nav_date":latest_date,
                 "3y_cagr": y3_cagr, 
                 "5y_cagr":y5_cagr
                 }
            )

        return jsonify({'data':response_data, 'message':'fetched successfully'}), 200

