import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG

def get_db_connection():
    """データベース接続を取得"""
    return psycopg2.connect(**DB_CONFIG)
