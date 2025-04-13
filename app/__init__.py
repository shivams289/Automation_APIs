from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from .extensions import db
# from flask_migrate import Migrate
from dotenv import load_dotenv
import os
from .api.research import research_bp
from .api.research import db_conn_bp
from .api.research import returns_bp
from .api.ranker import top_funds_momentum_bp, top_funds_outperf_bp
from .api.health import health_bp

# from .api.users import users_bp
# from .api.items import items_bp

# Load environment variables
load_dotenv()

# Initialize extensions
# db = SQLAlchemy()
# migrate = Migrate()

def create_app():
    app = Flask(__name__)

    # Load configuration
    app.config.from_object('app.config.Config')

    # Initialize extensions with app
    db.init_app(app)

    # migrate.init_app(app, db)

    # Register Blueprints
    app.register_blueprint(health_bp)
    app.register_blueprint(research_bp, url_prefix = '/api/research')
    app.register_blueprint(db_conn_bp, url_prefix = '/api/research')
    app.register_blueprint(returns_bp, url_prefix = '/api/research')
    app.register_blueprint(top_funds_momentum_bp, url_prefix = '/api/research')
    app.register_blueprint(top_funds_outperf_bp, url_prefix = '/api/research')
    # app.register_blueprint(users_bp, url_prefix='/users')
    # app.register_blueprint(items_bp, url_prefix='/items')

    return app
