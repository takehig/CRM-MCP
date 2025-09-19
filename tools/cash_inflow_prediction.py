# Cash inflow prediction tool from sales notes

import time
import json
from typing import Dict, Any, List
from utils.database import get_db_connection
from utils.system_prompt import get_system_prompt
from utils.llm_util import llm_util
from models import MCPResponse
from psycopg2.extras import RealDictCursor

async def predict_cash_inflow_from_sales_notes(params: Dict[str, Any]) -> MCPResponse:
    """営業メモから入金予測を抽出"""
    start_time = time.time()
    
    print(f"[predict_cash_inflow_from_sales_notes] === FUNCTION START ===")
    print(f"[predict_cash_inflow_from_sales_notes] Received raw params: {params}")
    
    # Try外で初期化（エラー時情報保持）
    tool_debug = {
        "standardize_prompt": None,
        "standardize_response": None,
        "standardize_parameter": None,
        "executed_query": None,
        "executed_query_results": None,
        "format_request": None,
        "format_response": None,
        "execution_time_ms": None,
        "results_count": None,
        "customers_analyzed": None,
        "llm_analysis_calls": None,
        "predictions_found": None,
        "individual_analysis": []
    }
    
    try:
        # 引数標準化処理（参照渡し）
        standardized_params = await standardize_cash_inflow_prediction_arguments(str(params), tool_debug)
        print(f"[predict_cash_inflow_from_sales_notes] Standardized params: {standardized_params}")
        
        customer_ids = standardized_params.get("customer_ids", [])
        
        # ビジネスロジック実行（参照渡し）
        predictions = await execute_cash_inflow_prediction_logic(customer_ids, tool_debug)
        
        # 結果フォーマット処理（参照渡し）
        result_text = await format_cash_inflow_prediction_results(predictions, str(params), tool_debug)
        
        execution_time = time.time() - start_time
        tool_debug["execution_time_ms"] = round(execution_time * 1000, 2)
        tool_debug["results_count"] = len(predictions)
        
        print(f"[predict_cash_inflow_from_sales_notes] Returning result with {len(predictions)} predictions")
        print(f"[predict_cash_inflow_from_sales_notes] === FUNCTION END ===")
        
        return MCPResponse(result=result_text, debug_response=tool_debug)
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_message = f"入金予測エラー: {str(e)}"
        
        tool_debug["execution_time_ms"] = round(execution_time * 1000, 2)
        tool_debug["results_count"] = 0
        
        print(f"[predict_cash_inflow_from_sales_notes] ERROR: {error_message}")
        print(f"[predict_cash_inflow_from_sales_notes] === FUNCTION END ===")
        
        return MCPResponse(result=error_message, debug_response=tool_debug, error=str(e))

async def standardize_cash_inflow_prediction_arguments(raw_input: str, tool_debug: Dict) -> Dict[str, Any]:
    """入金予測の引数を標準化（LLMベース）"""
    print(f"[standardize_cash_inflow_prediction_arguments] Raw input: {raw_input}")
    
    # データベースからシステムプロンプト取得
    system_prompt = await get_system_prompt("cash_inflow_prediction_pre")
    
    print(f"[standardize_cash_inflow_prediction_arguments] === LLM CALL START ===")
    response = await llm_util.call_claude(system_prompt, raw_input)
    print(f"[standardize_cash_inflow_prediction_arguments] LLM Raw Response: {response}")
    print(f"[standardize_cash_inflow_prediction_arguments] === LLM CALL END ===")
    
    full_prompt_text = f"{system_prompt}\n\nUser Input: {raw_input}"
    
    # tool_debugに情報設定
    tool_debug["standardize_prompt"] = full_prompt_text
    tool_debug["standardize_response"] = response
    
    try:
        standardized_params = json.loads(response)
        print(f"[standardize_cash_inflow_prediction_arguments] Final Standardized Output: {standardized_params}")
        tool_debug["standardize_parameter"] = str(standardized_params)
        return standardized_params
    except json.JSONDecodeError as e:
        print(f"[standardize_cash_inflow_prediction_arguments] JSON parse error: {e}")
        tool_debug["standardize_parameter"] = f"JSONパースエラー: {str(e)}"
        return {"customer_ids": []}

async def execute_cash_inflow_prediction_logic(customer_ids: List[int], tool_debug: Dict) -> List[Dict]:
    """営業メモ取得→LLMループ解析→入金予測抽出"""
    
    if not customer_ids:
        print("[execute_cash_inflow_prediction_logic] No customer IDs provided")
        tool_debug["customers_analyzed"] = 0
        tool_debug["llm_analysis_calls"] = 0
        tool_debug["predictions_found"] = 0
        return []
    
    # データベース接続・営業メモ取得
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # 顧客IDリストでクエリ構築
    placeholders = ','.join(['%s'] * len(customer_ids))
    query = f"""
    SELECT c.customer_id, c.name, sn.content as sales_note
    FROM customers c 
    JOIN sales_notes sn ON c.customer_id = sn.customer_id 
    WHERE c.customer_id IN ({placeholders})
    ORDER BY c.customer_id
    """
    
    print(f"[execute_cash_inflow_prediction_logic] Query: {query}")
    print(f"[execute_cash_inflow_prediction_logic] Customer IDs: {customer_ids}")
    
    # tool_debugにクエリ情報設定
    tool_debug["executed_query"] = query
    
    cursor.execute(query, customer_ids)
    customers_with_notes = cursor.fetchall()
    conn.close()
    
    print(f"[execute_cash_inflow_prediction_logic] Found {len(customers_with_notes)} customers with sales notes")
    
    # 営業メモ解析用システムプロンプト取得
    analysis_prompt = await get_system_prompt("cash_inflow_prediction_analysis")
    
    predictions = []
    individual_analysis = []
    llm_calls = 0
    predictions_found = 0
    
    # 各顧客の営業メモをLLMで解析
    for customer in customers_with_notes:
        print(f"[execute_cash_inflow_prediction_logic] Analyzing customer {customer['customer_id']}: {customer['name']}")
        
        try:
            # LLMで営業メモ解析
            prediction_response = await llm_util.call_claude(analysis_prompt, customer['sales_note'])
            llm_calls += 1
            
            print(f"[execute_cash_inflow_prediction_logic] LLM Response for customer {customer['customer_id']}: {prediction_response}")
            
            # JSON解析
            parsed_prediction = json.loads(prediction_response)
            
            # 個別解析結果保存
            individual_analysis.append({
                "customer_id": customer['customer_id'],
                "customer_name": customer['name'],
                "llm_response": prediction_response,
                "parsed_amount": parsed_prediction.get("amount"),
                "parsed_date": parsed_prediction.get("date")
            })
            
            # 予測データ構築
            prediction_data = {
                "customer_id": customer['customer_id'],
                "customer_name": customer['name'],
                "predicted_amount": parsed_prediction.get("amount"),
                "predicted_date": parsed_prediction.get("date")
            }
            
            predictions.append(prediction_data)
            
            # 予測が見つかった場合のカウント
            if parsed_prediction.get("amount") is not None:
                predictions_found += 1
                
        except Exception as e:
            print(f"[execute_cash_inflow_prediction_logic] Error analyzing customer {customer['customer_id']}: {e}")
            
            # エラー時も結果に含める
            individual_analysis.append({
                "customer_id": customer['customer_id'],
                "customer_name": customer['name'],
                "error": str(e)
            })
            
            predictions.append({
                "customer_id": customer['customer_id'],
                "customer_name": customer['name'],
                "predicted_amount": None,
                "predicted_date": None
            })
    
    # tool_debugに統計情報設定
    tool_debug["customers_analyzed"] = len(customers_with_notes)
    tool_debug["llm_analysis_calls"] = llm_calls
    tool_debug["predictions_found"] = predictions_found
    tool_debug["individual_analysis"] = individual_analysis
    tool_debug["executed_query_results"] = predictions
    
    return predictions

async def format_cash_inflow_prediction_results(predictions: list, user_input: str, tool_debug: Dict) -> str:
    """入金予測結果をテキスト化"""
    
    if not predictions:
        return "入金予測分析結果: 該当する顧客の営業メモが見つかりませんでした。"
    
    # データベースからシステムプロンプト取得
    system_prompt = await get_system_prompt("cash_inflow_prediction_post")
    
    # データJSON化
    data_json = json.dumps(predictions, ensure_ascii=False, default=str, indent=2)
    full_prompt = f"{system_prompt}\n\nData:\n{data_json}"
    
    # tool_debugにformat_request設定
    tool_debug["format_request"] = full_prompt
    
    # LLM呼び出し
    result_text, execution_time = await llm_util.call_llm_simple(full_prompt)
    print(f"[format_cash_inflow_prediction_results] Execution time: {execution_time}ms")
    print(f"[format_cash_inflow_prediction_results] Formatted result: {result_text[:200]}...")
    
    # tool_debugにformat_response設定
    tool_debug["format_response"] = result_text
    
    return result_text
