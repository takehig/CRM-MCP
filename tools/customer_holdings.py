# Customer holdings tool

import time
import json
from typing import Dict, Any, Tuple, List
from utils.database import get_db_connection, get_system_prompt
from utils import llm_util
from models import MCPResponse
from psycopg2.extras import RealDictCursor

async def standardize_customer_arguments(raw_input: str) -> Tuple[list, str, str, str]:
    """顧客検索の引数を標準化（LLMベース）"""
    print(f"[standardize_customer_arguments] Raw input: {raw_input}")
    
    # データベースからシステムプロンプト取得
    system_prompt = await get_system_prompt("get_customer_holdings_pre")
    
    # 完全プロンプト作成
    full_prompt = f"{system_prompt}\n\nUser Input: {raw_input}"
    
    # call_llm_simple使用（統一）
    response, execution_time = await llm_util.call_llm_simple(full_prompt)
    print(f"[standardize_customer_arguments] LLM Raw Response: {response}")
    print(f"[standardize_customer_arguments] Execution time: {execution_time}ms")
    
    full_prompt_text = full_prompt
    
    try:
        customer_ids = json.loads(response)
        if not isinstance(customer_ids, list):
            customer_ids = []
        print(f"[standardize_customer_arguments] Final Customer IDs: {customer_ids}")
        return customer_ids, full_prompt_text, response, str(customer_ids)
    except json.JSONDecodeError as e:
        print(f"[standardize_customer_arguments] JSON parse error: {e}")
        return [], full_prompt_text, response, f"LLM応答のJSONパース失敗: {str(e)}"

async def format_customer_holdings_results(holdings: list) -> str:
    """顧客保有商品結果をテキスト化"""
    if not holdings:
        return "保有商品検索結果: 該当する保有商品はありませんでした。"
    
    # システムプロンプト取得
    system_prompt = await llm_util.get_system_prompt('get_customer_holdings_post')
    
    # 呼び出し元でデータ結合（責任明確化）
    data_json = json.dumps(holdings, ensure_ascii=False, default=str, indent=2)
    full_prompt = f"{system_prompt}\n\nData:\n{data_json}"
    
    # 完全プロンプトでLLM呼び出し
    result_text, execution_time = await llm_util.call_llm_simple(full_prompt)
    print(f"[format_customer_holdings_results] Execution time: {execution_time}ms")
    print(f"[format_customer_holdings_results] Formatted result: {result_text[:200]}...")
    
    return result_text

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
        
        # 結果テキスト化（関数化）
        result_text = await format_customer_holdings_results(holdings)
        
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
