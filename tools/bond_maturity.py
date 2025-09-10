# Bond maturity search tool

import time
from typing import Dict, Any, List
from utils.database import get_db_connection
from utils.standardizers import standardize_bond_maturity_arguments
from utils.llm_client import bedrock_client
from models import MCPResponse
from psycopg2.extras import RealDictCursor

async def _format_bond_maturity_results(customers_array: List[Dict]) -> str:
    """債券満期検索結果の専用フォーマット処理"""
    if not customers_array:
        return "債券満期検索結果: 該当する顧客はいませんでした。"
    
    system_prompt = """債券満期検索の結果配列を、後続のツールが使いやすいシンプルなテキスト形式に変換してください。

要求:
1. 顧客IDを明確に記載
2. 満期日情報を含める
3. 後続ツールが顧客IDを抽出しやすい形式

例:
顧客ID: 1, 2, 3
満期債券保有者: 伊藤正雄(ID:1, 満期:2025-12-31), 田中花子(ID:2, 満期:2026-06-30)"""

    customers_json = str(customers_array)
    result_text = await bedrock_client.call_claude(system_prompt, customers_json)
    
    print(f"[_format_bond_maturity_results] Formatted result: {result_text[:200]}...")
    return result_text

async def search_customers_by_bond_maturity(params: Dict[str, Any]) -> MCPResponse:
    """債券満期日条件での顧客検索"""
    start_time = time.time()
    
    print(f"[search_customers_by_bond_maturity] === FUNCTION START ===")
    print(f"[search_customers_by_bond_maturity] Received raw params: {params}")
    
    try:
        # 引数標準化処理
        standardized_params, full_prompt_text, standardize_response, standardize_parameter = await standardize_bond_maturity_arguments(str(params))
        print(f"[search_customers_by_bond_maturity] Standardized params: {standardized_params}")
        
        days_until_maturity = standardized_params.get("days_until_maturity")
        maturity_date_from = standardized_params.get("maturity_date_from")
        maturity_date_to = standardized_params.get("maturity_date_to")
        
        print(f"[search_customers_by_bond_maturity] Extracted values:")
        print(f"  - days_until_maturity: {days_until_maturity}")
        print(f"  - maturity_date_from: {maturity_date_from}")
        print(f"  - maturity_date_to: {maturity_date_to}")
        
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
        
        # テキスト化
        result_text = await _format_bond_maturity_results(customers)
        
        execution_time = time.time() - start_time
        
        # debug_response作成
        tool_debug = {
            "executed_query": query,
            "executed_query_results": customers,
            "format_response": result_text,
            "standardize_prompt": full_prompt_text,
            "standardize_response": standardize_response,
            "standardize_parameter": standardize_parameter,
            "execution_time_ms": round(execution_time * 1000, 2),
            "results_count": len(customers)
        }
        
        print(f"[search_customers_by_bond_maturity] Returning result with {len(customers)} customers")
        print(f"[search_customers_by_bond_maturity] === FUNCTION END ===")
        
        return MCPResponse(result=result_text, debug_response=tool_debug)
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_message = f"債券満期検索エラー: {str(e)}"
        
        tool_debug = {
            "error": str(e),
            "error_type": type(e).__name__,
            "execution_time_ms": round(execution_time * 1000, 2),
            "results_count": 0
        }
        
        print(f"[search_customers_by_bond_maturity] Error: {e}")
        print(f"[search_customers_by_bond_maturity] === FUNCTION END (ERROR) ===")
        
        return MCPResponse(result=error_message, debug_response=tool_debug)
