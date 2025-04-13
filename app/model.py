from .extensions import db

class PerformanceHistory(db.Model):
    __tablename__ = 'performance_history'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, nullable=False)
    scheme_id = db.Column(db.String(255), nullable=True)
    nav = db.Column(db.Float, nullable=True)
    date = db.Column(db.Integer, nullable=True)
    created_dt = db.Column(db.Integer, nullable=True)
    created_by = db.Column(db.Integer, nullable=True)
    updated_dt = db.Column(db.Integer, nullable=True)
    updated_by = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<Fund ID {self.scheme_id}>"
