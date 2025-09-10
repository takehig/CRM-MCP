# Argument standardizers

import json
from typing import Dict, Any, Tuple
from utils.llm_client import bedrock_client

async def standardize_bond_maturity_arguments(raw_input: str) -> Tuple[Dict[str, Any], str, str, str]:
    """債券満期日検索の引数を標準化（LLMベース）"""
    print(f"[standardize_bond_maturity_arguments] Raw input: {raw_input}")
    
    system_prompt = """あなたは債券満期日検索の引数標準化エージェントです。

不定形の入力を以下の標準形式に変換してください：

標準形式（いずれか1つ以上を出力）:
- days_until_maturity: 満期までの日数（整数）
- maturity_date_from: 開始日（YYYY-MM-DD形式）
- maturity_date_to: 終了日（YYYY-MM-DD形式）

変換例:
入力: "{'maturity_range': '2 years'}" → 出力: {"days_until_maturity": 730}
入力: "{'period': '18ヶ月'}" → 出力: {"days_until_maturity": 540}
入力: "{'within': '6 months'}" → 出力: {"days_until_maturity": 180}
入力: "{'from': '2025-01-01', 'to': '2026-12-31'}" → 出力: {"maturity_date_from": "2025-01-01", "maturity_date_to": "2026-12-31"}

どんな形式の入力でも強引に標準形式に変換してください。
JSON形式のみで回答してください。"""

    full_prompt_text = f"{system_prompt}\n\nUser Input: {raw_input}"
    
    print(f"[standardize_bond_maturity_arguments] === LLM CALL START ===")
    response = await bedrock_client.call_claude(system_prompt, raw_input)
    print(f"[standardize_bond_maturity_arguments] LLM Raw Response: {response}")
    print(f"[standardize_bond_maturity_arguments] === LLM CALL END ===")
    
    try:
        standardized_params = json.loads(response)
        print(f"[standardize_bond_maturity_arguments] Final Standardized Output: {standardized_params}")
        return standardized_params, full_prompt_text, response, str(standardized_params)
    except json.JSONDecodeError as e:
        print(f"[standardize_bond_maturity_arguments] JSON parse error: {e}")
        return {}, full_prompt_text, response, f"JSONパースエラー: {str(e)}"

async def standardize_customer_arguments(raw_input: str) -> Tuple[list, str, str, str]:
    """顧客検索の引数を標準化（LLMベース）"""
    print(f"[standardize_customer_arguments] Raw input: {raw_input}")
    
    system_prompt = """入力テキストから顧客IDを抽出してください。
JSON配列形式で回答:
["ID1", "ID2", "ID3"]

例:
- "顧客ID: 1, 7" → ["1", "7"]
- "伊藤正雄さんの保有商品" → ["1"]
- "全顧客" → []"""

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
