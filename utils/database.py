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

async def get_system_prompt(prompt_key: str) -> str:
    """システムプロンプトをデータベースから取得"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT prompt_text FROM system_prompts WHERE prompt_key = %s",
            (prompt_key,)
        )
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            print(f"[get_system_prompt] Found prompt for key: {prompt_key}")
            return result[0]
        else:
            print(f"[get_system_prompt] No prompt found for key: {prompt_key}, using fallback")
            # フォールバック用のデフォルトプロンプト
            if "pre" in prompt_key:
                return "Convert input to standard format. Respond in JSON only."
            else:
                return "Format the results in a readable text format."
                
    except Exception as e:
        print(f"[ERROR] Failed to get system prompt: {e}")
        # エラー時のフォールバック
        if "pre" in prompt_key:
            return "Convert input to standard format. Respond in JSON only."
        else:
            return "Format the results in a readable text format."
