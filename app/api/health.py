from flask import Blueprint, jsonify

health_bp = Blueprint('health', __name__)

@health_bp.route('/health', methods=['GET'])
def health_check():
    response = {
        'status': 'OK',
        'message': 'The server is healthy!'
    }
    return jsonify(response), 200