# Database utilities

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException
from config import DB_CONFIG
import httpx
import logging

logger = logging.getLogger(__name__)

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
    """システムプロンプトをAIChat API経由で取得"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"http://localhost:8002/api/system-prompts/{prompt_key}",
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"[get_system_prompt] Found prompt for key: {prompt_key}")
                return data["prompt_text"]
            elif response.status_code == 404:
                print(f"[get_system_prompt] No prompt found for key: {prompt_key}, using fallback")
                return _get_fallback_prompt(prompt_key)
            else:
                print(f"[get_system_prompt] API error {response.status_code}, using fallback")
                return _get_fallback_prompt(prompt_key)
                
    except Exception as e:
        logger.error(f"Failed to get system prompt from API: {e}")
        print(f"[ERROR] Failed to get system prompt: {e}")
        return _get_fallback_prompt(prompt_key)

def _get_fallback_prompt(prompt_key: str) -> str:
    """フォールバック用のデフォルトプロンプト"""
    if "pre" in prompt_key:
        return "Convert input to standard format. Respond in JSON only."
    else:
        return "Format the results in a readable text format."
