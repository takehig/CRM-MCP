# Customer holdings tool

import time
import json
from typing import Dict, Any, Tuple, List
from utils.database import get_db_connection
from utils.system_prompt import get_system_prompt
from utils.llm_util import llm_util
from models import MCPResponse
from psycopg2.extras import RealDictCursor

async def get_customer_holdings(params: Dict[str, Any]) -> MCPResponse:
    """顧客の保有商品情報を取得"""
    start_time = time.time()
    
    print(f"[get_customer_holdings] === FUNCTION START ===")
    print(f"[get_customer_holdings] Received raw params: {params}")
    
    # デバッグ情報インスタンス（try外側で定義）
    tool_debug = {
        "request": params,
        "standardize_prompt": None,
        "standardize_response": None,
        "standardize_parameter": None,
        "executed_query": None,
        "executed_query_results": None,
        "format_response": None,
        "error": None,
        "error_type": None,
        "execution_time_ms": 0,
        "results_count": 0
    }
    
    try:
        # 引数標準化処理（顧客ID抽出）
        await standardize_customer_arguments(str(params), tool_debug)
        
        if not tool_debug.get("customer_ids"):
            tool_debug["error"] = "顧客ID抽出失敗"
            tool_debug["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
            
            return MCPResponse(
                result="顧客特定不可のため実行できませんでした",
                debug_response=tool_debug
            )
        
        # データベースクエリ実行
        await execute_holdings_query(tool_debug)
        
        # 結果テキスト化
        await format_customer_holdings_results(tool_debug)
        
        tool_debug["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
        
        print(f"[get_customer_holdings] Returning result with {tool_debug['results_count']} holdings")
        print(f"[get_customer_holdings] === FUNCTION END ===")
        
        return MCPResponse(
            result=tool_debug["format_response"], 
            debug_response=tool_debug
        )
        
    except Exception as e:
        tool_debug["error"] = str(e)
        tool_debug["error_type"] = type(e).__name__
        tool_debug["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
        
        print(f"[get_customer_holdings] Error: {e}")
        print(f"[get_customer_holdings] === FUNCTION END (ERROR) ===")
        
        return MCPResponse(
            result=f"顧客保有商品取得エラー: {str(e)}", 
            debug_response=tool_debug
        )

async def standardize_customer_arguments(raw_input: str, tool_debug: dict) -> None:
    """顧客検索の引数を標準化（LLMベース）- 参照渡し"""
    print(f"[standardize_customer_arguments] Raw input: {raw_input}")
    
    # データベースからシステムプロンプト取得
    system_prompt = await get_system_prompt("get_customer_holdings_pre")
    
    # 完全プロンプト作成
    full_prompt = f"{system_prompt}\n\nUser Input: {raw_input}"
    tool_debug["standardize_prompt"] = full_prompt
    
    # call_llm_simple使用（統一）
    response, execution_time = await llm_util.call_llm_simple(full_prompt)
    tool_debug["standardize_response"] = response
    
    print(f"[standardize_customer_arguments] LLM Raw Response: {response}")
    print(f"[standardize_customer_arguments] Execution time: {execution_time}ms")
    
    try:
        customer_ids = json.loads(response)
        if not isinstance(customer_ids, list):
            customer_ids = []
        
        tool_debug["customer_ids"] = customer_ids
        tool_debug["standardize_parameter"] = str(customer_ids)
        
        print(f"[standardize_customer_arguments] Final Customer IDs: {customer_ids}")
        
    except json.JSONDecodeError as e:
        print(f"[standardize_customer_arguments] JSON parse error: {e}")
        tool_debug["customer_ids"] = []
        tool_debug["standardize_parameter"] = f"LLM応答のJSONパース失敗: {str(e)}"

async def execute_holdings_query(tool_debug: dict) -> None:
    """データベースクエリ実行 - 参照渡し"""
    customer_ids = tool_debug["customer_ids"]
    
    # データベース接続・クエリ実行
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # OR条件でSQL構築
    placeholders = ",".join(["%s"] * len(customer_ids))
    query = f"""
    SELECT h.holding_id, h.quantity, h.unit_price, h.current_price, h.current_value,
           h.purchase_date, h.customer_id,
           p.product_code, p.product_name, p.category_code, p.currency,
           c.name as customer_name
    FROM holdings h
    JOIN products p ON h.product_id = p.product_id
    JOIN customers c ON h.customer_id = c.customer_id
    WHERE h.customer_id IN ({placeholders})
    ORDER BY h.customer_id, h.current_value DESC
    """
    
    tool_debug["executed_query"] = query
    
    print(f"[execute_holdings_query] Final query: {query}")
    print(f"[execute_holdings_query] Customer IDs: {customer_ids}")
    
    cursor.execute(query, customer_ids)
    results = cursor.fetchall()
    conn.close()
    
    print(f"[execute_holdings_query] Query executed, found {len(results)} holdings")
    
    # 結果配列作成
    holdings = []
    for row in results:
        holdings.append({
            "holding_id": row['holding_id'],
            "customer_id": row['customer_id'],
            "customer_name": row['customer_name'],
            "product_code": row['product_code'],
            "product_name": row['product_name'],
            "category_code": row['category_code'],
            "quantity": float(row['quantity']) if row['quantity'] else 0,
            "unit_price": float(row['unit_price']) if row['unit_price'] else 0,
            "current_price": float(row['current_price']) if row['current_price'] else 0,
            "current_value": float(row['current_value']) if row['current_value'] else 0,
            "currency": row['currency'],
            "purchase_date": row['purchase_date'].isoformat() if row['purchase_date'] else None
        })
    
    tool_debug["executed_query_results"] = holdings
    tool_debug["results_count"] = len(holdings)

async def format_customer_holdings_results(tool_debug: dict) -> None:
    """顧客保有商品結果をテキスト化 - 参照渡し"""
    holdings = tool_debug["executed_query_results"]
    
    if not holdings:
        tool_debug["format_response"] = "保有商品検索結果: 該当する保有商品はありませんでした。"
        return
    
    # システムプロンプト取得
    system_prompt = await get_system_prompt('get_customer_holdings_post')
    
    # 呼び出し元でデータ結合（責任明確化）
    data_json = json.dumps(holdings, ensure_ascii=False, default=str, indent=2)
    full_prompt = f"{system_prompt}\n\nData:\n{data_json}"
    
    # 完全プロンプトでLLM呼び出し
    result_text, execution_time = await llm_util.call_llm_simple(full_prompt)
    
    tool_debug["format_response"] = result_text
    
    print(f"[format_customer_holdings_results] Execution time: {execution_time}ms")
    print(f"[format_customer_holdings_results] Formatted result: {result_text[:200]}...")
