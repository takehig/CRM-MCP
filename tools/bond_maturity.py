# Bond maturity search tool

import time
import json
from typing import Dict, Any, Tuple, List
from utils.database import get_db_connection
from utils.system_prompt import get_system_prompt
from utils.llm_util import llm_util
from models import MCPResponse
from psycopg2.extras import RealDictCursor

async def search_customers_by_bond_maturity(params: Dict[str, Any]) -> MCPResponse:
    """債券満期日条件での顧客検索"""
    start_time = time.time()
    
    print(f"[search_customers_by_bond_maturity] === FUNCTION START ===")
    print(f"[search_customers_by_bond_maturity] Received raw params: {params}")
    
    # Try外で初期化（エラー時情報保持）
    tool_debug = {
        "format_request": None,
        "standardize_prompt": None,
        "standardize_response": None,
        "standardize_parameter": None,
        "executed_query": None,
        "executed_query_results": None,
        "format_response": None,
        "execution_time_ms": None,
        "results_count": None
    }
    
    try:
        # 引数標準化処理（参照渡し）
        standardized_params = await standardize_bond_maturity_arguments(str(params), tool_debug)
        print(f"[search_customers_by_bond_maturity] Standardized params: {standardized_params}")
        
        days_until_maturity = standardized_params.get("days_until_maturity")
        maturity_date_from = standardized_params.get("maturity_date_from")
        maturity_date_to = standardized_params.get("maturity_date_to")
        
        print(f"[search_customers_by_bond_maturity] Extracted values:")
        print(f"  - days_until_maturity: {days_until_maturity}")
        print(f"  - maturity_date_from: {maturity_date_from}")
        print(f"  - maturity_date_to: {maturity_date_to}")
        
        # データベース接続・クエリ実行（参照渡し）
        customers = await execute_bond_maturity_query(days_until_maturity, maturity_date_from, maturity_date_to, tool_debug)
        
        # 結果テキスト化（参照渡し）
        result_text = await format_bond_maturity_results(customers, str(params), tool_debug)
        
        execution_time = time.time() - start_time
        tool_debug["execution_time_ms"] = round(execution_time * 1000, 2)
        tool_debug["results_count"] = len(customers)
        
        print(f"[search_customers_by_bond_maturity] Returning result with {len(customers)} customers")
        print(f"[search_customers_by_bond_maturity] === FUNCTION END ===")
        
        return MCPResponse(result=result_text, debug_response=tool_debug)
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_message = f"債券満期検索エラー: {str(e)}"
        
        tool_debug["execution_time_ms"] = round(execution_time * 1000, 2)
        tool_debug["results_count"] = 0
        
        print(f"[search_customers_by_bond_maturity] ERROR: {error_message}")
        print(f"[search_customers_by_bond_maturity] === FUNCTION END ===")
        
        return MCPResponse(result=error_message, debug_response=tool_debug, error=str(e))

async def standardize_bond_maturity_arguments(raw_input: str, tool_debug: Dict) -> Dict[str, Any]:
    """債券満期日検索の引数を標準化（LLMベース）"""
    print(f"[standardize_bond_maturity_arguments] Raw input: {raw_input}")
    
    # データベースからシステムプロンプト取得
    system_prompt = await get_system_prompt("search_customers_by_bond_maturity_pre")
    
    print(f"[standardize_bond_maturity_arguments] === LLM CALL START ===")
    response = await llm_util.call_claude(system_prompt, raw_input)
    print(f"[standardize_bond_maturity_arguments] LLM Raw Response: {response}")
    print(f"[standardize_bond_maturity_arguments] === LLM CALL END ===")
    
    full_prompt_text = f"{system_prompt}\n\nUser Input: {raw_input}"
    
    # tool_debugに情報設定
    tool_debug["standardize_prompt"] = full_prompt_text
    tool_debug["standardize_response"] = response
    
    try:
        standardized_params = json.loads(response)
        print(f"[standardize_bond_maturity_arguments] Final Standardized Output: {standardized_params}")
        tool_debug["standardize_parameter"] = str(standardized_params)
        return standardized_params
    except json.JSONDecodeError as e:
        print(f"[standardize_bond_maturity_arguments] JSON parse error: {e}")
        tool_debug["standardize_parameter"] = f"JSONパースエラー: {str(e)}"
        return {}

async def execute_bond_maturity_query(days_until_maturity, maturity_date_from, maturity_date_to, tool_debug: Dict) -> List[Dict]:
    """債券満期クエリ実行"""
    # データベース接続・クエリ実行
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT DISTINCT c.customer_id, c.name, c.email, c.phone, c.risk_tolerance,
           h.maturity_date, p.product_name, p.product_type
    FROM customers c
    JOIN holdings h ON c.customer_id = h.customer_id
    JOIN products p ON h.product_id = p.product_id
    WHERE p.product_type ILIKE '%債券%' OR p.product_type ILIKE '%bond%'
    """
    query_params = []
    
    if days_until_maturity:
        query += f" AND h.maturity_date <= CURRENT_DATE + INTERVAL '{days_until_maturity} days'"
    
    if maturity_date_from:
        query += " AND h.maturity_date >= %s"
        query_params.append(maturity_date_from)
    
    if maturity_date_to:
        query += " AND h.maturity_date <= %s"
        query_params.append(maturity_date_to)
    
    query += " ORDER BY h.maturity_date ASC"
    
    print(f"[search_customers_by_bond_maturity] Final query: {query}")
    print(f"[search_customers_by_bond_maturity] Query params: {query_params}")
    
    # tool_debugにクエリ情報設定
    tool_debug["executed_query"] = query
    
    # SQL実行
    if query_params:
        cursor.execute(query, query_params)
    else:
        cursor.execute(query)
    
    results = cursor.fetchall()
    conn.close()
    
    print(f"[search_customers_by_bond_maturity] Query executed, found {len(results)} rows")
    
    # 結果配列作成
    customers = []
    for row in results:
        customers.append({
            "customer_id": row['customer_id'],
            "name": row['name'],
            "email": row['email'],
            "phone": row['phone'],
            "risk_tolerance": row['risk_tolerance'],
            "maturity_date": row['maturity_date'].isoformat() if row['maturity_date'] else None,
            "product_name": row['product_name'],
            "product_type": row['product_type']
        })
    
    # tool_debugに結果設定
    tool_debug["executed_query_results"] = customers
    
    return customers

async def format_bond_maturity_results(customers: list, user_input: str, tool_debug: Dict) -> str:
    """債券満期検索結果をテキスト化"""
    if not customers:
        return "債券満期検索結果: 該当する顧客はいませんでした。"
    
    # データベースからシステムプロンプト取得
    system_prompt = await get_system_prompt("search_customers_by_bond_maturity_post")
    
    # 呼び出し元でデータ結合（責任明確化）
    data_json = json.dumps(customers, ensure_ascii=False, default=str, indent=2)
    full_prompt = f"{system_prompt}\n\nData:\n{data_json}"
    
    # tool_debugにformat_request（full_prompt）を設定
    tool_debug["format_request"] = full_prompt
    
    # 完全プロンプトでLLM呼び出し
    result_text, execution_time = await llm_util.call_llm_simple(full_prompt)
    print(f"[format_bond_maturity_results] Execution time: {execution_time}ms")
    print(f"[format_bond_maturity_results] Formatted result: {result_text[:200]}...")
    
    # tool_debugにformat_responseを設定
    tool_debug["format_response"] = result_text
    
    return result_text
