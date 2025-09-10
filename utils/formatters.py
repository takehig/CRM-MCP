# Text formatters

from typing import List, Dict
from utils.llm_client import bedrock_client

async def format_customers_to_text(customers_array: List[Dict]) -> str:
    """債券満期検索結果配列をテキスト形式に変換"""
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
    
    print(f"[format_customers_to_text] Formatted result: {result_text[:200]}...")
    return result_text

async def format_holdings_to_text(holdings_array: List[Dict]) -> str:
    """保有商品配列をテキスト形式に変換"""
    if not holdings_array:
        return "保有商品検索結果: 該当する保有商品はありませんでした。"
    
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

    holdings_json = str(holdings_array)
    result_text = await bedrock_client.call_claude(system_prompt, holdings_json)
    
    print(f"[format_holdings_to_text] Formatted result: {result_text[:200]}...")
    return result_text
