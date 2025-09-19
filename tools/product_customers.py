"""
商品保有顧客抽出ツール
テキストから商品IDを抽出し、該当商品の保有顧客リストを返す
"""

import json
import time
import asyncio
import aiohttp
from utils.database import get_db_connection
from utils.llm_client import call_bedrock_llm
from models import MCPResponse

async def get_system_prompt(prompt_key: str) -> str:
    """SystemPrompt Management APIからプロンプトを取得"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://localhost:8007/api/prompts/{prompt_key}") as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("prompt_text", "")
                else:
                    return f"プロンプト取得エラー: {response.status}"
    except Exception as e:
        return f"プロンプト取得エラー: {str(e)}"

async def get_customers_by_product_text(search_result_text: str):
    """
    テキストから商品IDを抽出し、該当商品の保有顧客リストを返す
    
    Args:
        search_result_text: 商品IDを含むテキスト
        
    Returns:
        MCPResponse: 結果とデバッグ情報
    """
    start_total_time = time.time()
    
    # Try外側でデバッグ情報初期化（推奨構造）
    debug_response = {
        "function_name": "get_customers_by_product_text",
        "input_params": {"search_result_text": search_result_text},
        "step1_extract_ids": {
            "llm_request": None,
            "llm_response": None,
            "execution_time_ms": 0,
            "result": None
        },
        "step2_sql_execution": {
            "sql_query": None,
            "sql_parameters": None,
            "execution_time_ms": 0,
            "result": None
        },
        "step3_format_results": {
            "llm_request": None,
            "llm_response": None,
            "execution_time_ms": 0,
            "result": None
        },
        "total_execution_time_ms": 0,
        "error": None
    }
    
    try:
        # STEP 1: ID抽出（LLM使用）
        step1_start = time.time()
        extract_prompt = await get_system_prompt("customer_by_product_extract_ids")
        combined_request = f"{extract_prompt}\n\n入力テキスト: {search_result_text}"
        debug_response["step1_extract_ids"]["llm_request"] = combined_request
        
        ids_response = await call_bedrock_llm(extract_prompt, search_result_text)
        debug_response["step1_extract_ids"]["llm_response"] = ids_response
        debug_response["step1_extract_ids"]["execution_time_ms"] = int((time.time() - step1_start) * 1000)
        
        product_ids = json.loads(ids_response)
        debug_response["step1_extract_ids"]["result"] = product_ids
        
        if not product_ids:
            debug_response["error"] = "商品IDが抽出されませんでした"
            debug_response["total_execution_time_ms"] = int((time.time() - start_total_time) * 1000)
            return MCPResponse(
                result="商品IDが抽出されませんでした",
                error="商品IDが抽出されませんでした",
                debug_response=debug_response
            )
        
        # STEP 2: SQL実行（LLM使用しない）
        step2_start = time.time()
        placeholders = ','.join(['%s'] * len(product_ids))
        query = f"""
            SELECT h.product_id, p.product_name, h.customer_id, c.customer_name, 
                   h.quantity, h.current_value
            FROM holdings h 
            JOIN customers c ON h.customer_id = c.customer_id 
            JOIN products p ON h.product_id = p.product_id
            WHERE h.product_id IN ({placeholders})
            ORDER BY h.product_id, h.current_value DESC
        """
        debug_response["step2_sql_execution"]["sql_query"] = query
        debug_response["step2_sql_execution"]["sql_parameters"] = product_ids
        
        # データベース接続・実行
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, product_ids)
        
        # 結果を辞書形式で取得
        columns = [desc[0] for desc in cursor.description]
        customers = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        debug_response["step2_sql_execution"]["execution_time_ms"] = int((time.time() - step2_start) * 1000)
        debug_response["step2_sql_execution"]["result"] = customers
        
        # STEP 3: 結果整形（LLM使用）
        step3_start = time.time()
        format_prompt = await get_system_prompt("customer_by_product_format_results")
        customers_json = json.dumps(customers, ensure_ascii=False)
        combined_request = f"{format_prompt}\n\n入力データ: {customers_json}"
        debug_response["step3_format_results"]["llm_request"] = combined_request
        
        formatted_response = await call_bedrock_llm(format_prompt, customers_json)
        debug_response["step3_format_results"]["llm_response"] = formatted_response
        debug_response["step3_format_results"]["execution_time_ms"] = int((time.time() - step3_start) * 1000)
        debug_response["step3_format_results"]["result"] = formatted_response
        
        debug_response["total_execution_time_ms"] = int((time.time() - start_total_time) * 1000)
        
        return MCPResponse(
            result=formatted_response,
            debug_response=debug_response
        )
        
    except Exception as e:
        debug_response["error"] = str(e)
        debug_response["total_execution_time_ms"] = int((time.time() - start_total_time) * 1000)
        return MCPResponse(
            result=f"処理中にエラーが発生しました: {str(e)}",
            error=f"処理中にエラーが発生しました: {str(e)}",
            debug_response=debug_response
        )
