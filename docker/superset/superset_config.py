import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    "@postgres:5432/superset"
)
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
BABEL_DEFAULT_LOCALE = "en"
# Trino query trên vài triệu dòng có thể > 60s ở máy local
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
