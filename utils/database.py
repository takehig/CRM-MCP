# Database utilities

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException
from config import DB_CONFIG

def get_db_connection():
    """PostgreSQLデータベース接続"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

async def execute_query(query: str, params: list = None):
    """クエリ実行（共通処理）"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        conn.close()
        raise e
