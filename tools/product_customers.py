"""
商品保有顧客抽出ツール
テキストから商品IDを抽出し、該当商品の保有顧客リストを返す
"""

import json
import time
from utils.database import get_db_connection
from utils.llm_util import llm_util
from utils.system_prompt import get_system_prompt
from models import MCPResponse

async def get_customers_by_product_text(text_input: str):
    """
    テキストから商品IDを抽出し、該当商品の保有顧客リストを返す
    
    Args:
        text_input: 商品IDを含むテキスト
        
    Returns:
        MCPResponse: 結果とデバッグ情報
    """
    start_total_time = time.time()
    
    # Try外側でデバッグ情報初期化（推奨構造）
    debug_response = {
        "function_name": "get_customers_by_product_text",
        "input_params": {"text_input": text_input},  # 元の入力を記録
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
        # 呼び出し元責任：テキスト化処理
        if isinstance(text_input, dict):
            # 辞書の場合は適切な文字列を抽出
            if "text_input" in text_input:
                text_input_str = text_input["text_input"]
            else:
                text_input_str = str(text_input)
        else:
            text_input_str = str(text_input)
        
        # デバッグ情報に処理後の値も記録
        debug_response["processed_input"] = text_input_str
        
        # STEP 1: ID抽出（LLM使用）
        step1_start = time.time()
        extract_prompt = await get_system_prompt("customer_by_product_extract_ids")
        
        # 呼び出し元責任：プロンプト結合
        combined_request = f"{extract_prompt}\n\n入力テキスト: {text_input_str}"
        debug_response["step1_extract_ids"]["llm_request"] = combined_request
        
        # call_claude使用（system + user分離）
        ids_response = await llm_util.call_claude(extract_prompt, text_input_str)
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
        
        # 呼び出し元責任：プロンプト結合
        combined_request = f"{format_prompt}\n\n入力データ: {customers_json}"
        debug_response["step3_format_results"]["llm_request"] = combined_request
        
        # call_claude使用（system + user分離）
        formatted_response = await llm_util.call_claude(format_prompt, customers_json)
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
