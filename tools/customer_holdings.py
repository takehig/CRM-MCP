# Customer holdings tool

import time
import json
from typing import Dict, Any, Tuple, List
from utils.database import get_db_connection, get_system_prompt
from utils.llm_client import bedrock_client
from models import MCPResponse
from psycopg2.extras import RealDictCursor

async def standardize_customer_arguments(raw_input: str) -> Tuple[list, str, str, str]:
    """顧客検索の引数を標準化（LLMベース）"""
    print(f"[standardize_customer_arguments] Raw input: {raw_input}")
    
    # データベースからシステムプロンプト取得
    system_prompt = await get_system_prompt("get_customer_holdings_pre")
    
    full_prompt_text = f"{system_prompt}\n\nUser Input: {raw_input}"
    
    response = await bedrock_client.call_claude(system_prompt, raw_input)
    print(f"[standardize_customer_arguments] LLM Raw Response: {response}")
    
    try:
        customer_ids = json.loads(response)
        if not isinstance(customer_ids, list):
            customer_ids = []
        print(f"[standardize_customer_arguments] Final Customer IDs: {customer_ids}")
        return customer_ids, full_prompt_text, response, str(customer_ids)
    except json.JSONDecodeError as e:
        print(f"[standardize_customer_arguments] JSON parse error: {e}")
        return [], full_prompt_text, response, f"LLM応答のJSONパース失敗: {str(e)}"

async def get_customer_holdings(params: Dict[str, Any]) -> MCPResponse:
    """顧客の保有商品情報を取得"""
    start_time = time.time()
    
    print(f"[get_customer_holdings] === FUNCTION START ===")
    print(f"[get_customer_holdings] Received raw params: {params}")
    
    try:
        # 引数標準化処理（顧客ID抽出）
        customer_ids, full_prompt_text, standardize_response, standardize_parameter = await standardize_customer_arguments(str(params))
        print(f"[get_customer_holdings] Customer IDs: {customer_ids}")
        
        if not customer_ids:
            tool_debug = {
                "error": "顧客ID抽出失敗",
                "standardize_prompt": full_prompt_text,
                "standardize_response": standardize_response,
                "execution_time_ms": round((time.time() - start_time) * 1000, 2),
                "results_count": 0
            }
            
            return MCPResponse(
                result="顧客特定不可のため実行できませんでした",
                debug_response=tool_debug
            )
        
        # データベース接続・クエリ実行
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # OR条件でSQL構築
        placeholders = ",".join(["%s"] * len(customer_ids))
        query = f"""
        SELECT h.holding_id, h.quantity, h.unit_price, h.current_price, h.current_value,
               h.purchase_date, h.customer_id,
               p.product_code, p.product_name, p.product_type, p.currency,
               c.name as customer_name
        FROM holdings h
        JOIN products p ON h.product_id = p.product_id
        JOIN customers c ON h.customer_id = c.customer_id
        WHERE h.customer_id IN ({placeholders})
        ORDER BY h.customer_id, h.current_value DESC
        """
        
        print(f"[get_customer_holdings] Final query: {query}")
        print(f"[get_customer_holdings] Customer IDs: {customer_ids}")
        
        cursor.execute(query, customer_ids)
        results = cursor.fetchall()
        conn.close()
        
        print(f"[get_customer_holdings] Query executed, found {len(results)} holdings")
        
        # 結果配列作成
        holdings = []
        for row in results:
            holdings.append({
                "holding_id": row['holding_id'],
                "customer_id": row['customer_id'],
                "customer_name": row['customer_name'],
                "product_code": row['product_code'],
                "product_name": row['product_name'],
                "product_type": row['product_type'],
                "quantity": float(row['quantity']) if row['quantity'] else 0,
                "unit_price": float(row['unit_price']) if row['unit_price'] else 0,
                "current_price": float(row['current_price']) if row['current_price'] else 0,
                "current_value": float(row['current_value']) if row['current_value'] else 0,
                "currency": row['currency'],
                "purchase_date": row['purchase_date'].isoformat() if row['purchase_date'] else None
            })
        
        # テキスト化（直接処理）
        if not holdings:
            result_text = "保有商品検索結果: 該当する保有商品はありませんでした。"
        else:
            system_prompt = """保有商品の結果配列を、読みやすいテキスト形式に変換してください。

要求:
1. 顧客別にグループ化
2. 商品名、数量、現在価値を含める
3. 合計金額を計算

例:
顧客ID: 1 (伊藤正雄)
- 商品A: 100株, 現在価値: 1,000,000円
- 商品B: 50口, 現在価値: 500,000円
小計: 1,500,000円"""

            holdings_json = str(holdings)
            result_text = await bedrock_client.call_claude(system_prompt, holdings_json)
            print(f"[get_customer_holdings] Formatted result: {result_text[:200]}...")
        
        execution_time = time.time() - start_time
        
        # debug_response作成
        tool_debug = {
            "executed_query": query,
            "executed_query_results": holdings,
            "format_response": result_text,
            "standardize_prompt": full_prompt_text,
            "standardize_response": standardize_response,
            "standardize_parameter": standardize_parameter,
            "execution_time_ms": round(execution_time * 1000, 2),
            "results_count": len(holdings)
        }
        
        print(f"[get_customer_holdings] Returning result with {len(holdings)} holdings")
        print(f"[get_customer_holdings] === FUNCTION END ===")
        
        return MCPResponse(result=result_text, debug_response=tool_debug)
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_message = f"顧客保有商品取得エラー: {str(e)}"
        
        tool_debug = {
            "error": str(e),
            "error_type": type(e).__name__,
            "execution_time_ms": round(execution_time * 1000, 2),
            "results_count": 0
        }
        
        print(f"[get_customer_holdings] Error: {e}")
        print(f"[get_customer_holdings] === FUNCTION END (ERROR) ===")
        
        return MCPResponse(result=error_message, debug_response=tool_debug)
