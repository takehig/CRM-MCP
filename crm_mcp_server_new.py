#!/usr/bin/env python3
"""
CRM MCP Server - 顧客情報・営業データ検索API
Port: 8004
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import boto3

# Bedrock設定
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')

async def call_claude(system_prompt: str, user_message: str) -> str:
    """Claude API呼び出し"""
    try:
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4000,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_message}]
        }
        
        response = bedrock_client.invoke_model(
            modelId="anthropic.claude-3-sonnet-20240229-v1:0",
            body=json.dumps(body)
        )
        
        response_body = json.loads(response['body'].read())
        return response_body['content'][0]['text']
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return "{}"

async def standardize_customer_holdings_arguments(text_input: str) -> Dict[str, Any]:
    """顧客保有商品検索の条件を正規化"""
    system_prompt = """顧客保有商品検索の条件を正規化してください。

入力テキストから以下を抽出:
- 顧客ID（複数可能）
- 商品タイプフィルター
- 複数顧客の場合はOR条件として統合

JSON形式で回答:
{"customer_ids": ["ID1", "ID2"], "product_type_filter": "タイプ", "search_logic": "OR条件の説明"}

例:
- "顧客ID: C001, C002" → {"customer_ids": ["C001", "C002"], "product_type_filter": "", "search_logic": "複数顧客のOR検索"}
- "C001の債券" → {"customer_ids": ["C001"], "product_type_filter": "債券", "search_logic": "単一顧客の債券フィルター"}"""
    
    try:
        response = await call_claude(system_prompt, text_input)
        return json.loads(response)
    except:
        return {"customer_ids": [], "product_type_filter": "", "search_logic": "解析失敗"}

async def standardize_product_details_arguments(text_input: str) -> Dict[str, Any]:
    """商品詳細検索の条件を正規化"""
    system_prompt = """商品詳細検索の条件を正規化してください。

入力テキストから以下を抽出:
- 商品コード（複数可能）
- 商品名での検索
- 複数商品の場合はOR条件として統合

JSON形式で回答:
{"product_codes": ["CODE1", "CODE2"], "product_names": ["名前1"], "search_logic": "OR条件の説明"}

例:
- "商品コード: JP001, JP002" → {"product_codes": ["JP001", "JP002"], "product_names": [], "search_logic": "複数商品コードのOR検索"}
- "日本国債" → {"product_codes": [], "product_names": ["日本国債"], "search_logic": "商品名での検索"}"""
    
    try:
        response = await call_claude(system_prompt, text_input)
        return json.loads(response)
    except:
        return {"product_codes": [], "product_names": [], "search_logic": "解析失敗"}

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CRM MCP Server", version="1.0.0")

# CORS設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# データベース接続設定
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'wealthai',
    'user': 'wealthai_user',
    'password': 'wealthai123'
}

class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: int
    method: str
    params: Dict[str, Any] = {}

class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[int] = None
    result: Any = None
    error: Optional[str] = None
    debug_response: Optional[Dict[str, Any]] = None

def get_db_connection():
    """PostgreSQLデータベース接続"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    return {"status": "healthy", "service": "CRM-MCP", "timestamp": datetime.now().isoformat()}

@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """標準MCPプロトコル対応エンドポイント"""
    progress = "INIT"
    method = None
    params = None
    tool_name = None
    arguments = None
    
    print(f"[MCP_ENDPOINT] === REQUEST START ===")
    print(f"[MCP_ENDPOINT] Request received: {request}")
    print(f"[MCP_ENDPOINT] Request method: {request.method}")
    print(f"[MCP_ENDPOINT] Request params: {request.params}")
    
    try:
        print(f"[MCP_ENDPOINT] Entering try block")
        progress = "EXTRACTING_METHOD"
        method = request.method
        print(f"[MCP_ENDPOINT] Method extracted: {method}")
        
        progress = "EXTRACTING_PARAMS"
        params = request.params
        print(f"[MCP_ENDPOINT] Params extracted: {params}")
        
        if method == "tools/call":
            print(f"[MCP_ENDPOINT] Processing tools/call")
            progress = "TOOLS_CALL_PROCESSING"
            
            # 標準MCPプロトコル: tools/call
            progress = "EXTRACTING_TOOL_NAME"
            tool_name = params.get("name")
            print(f"[MCP_ENDPOINT] Tool name: {tool_name}")
            
            progress = "EXTRACTING_ARGUMENTS"
            arguments = params.get("arguments", {})
            print(f"[MCP_ENDPOINT] Arguments: {arguments}")
            
            if tool_name == "search_customers_by_bond_maturity":
                print(f"[MCP_ENDPOINT] Calling search_customers_by_bond_maturity")
                progress = "EXECUTING_SEARCH_CUSTOMERS_BY_BOND_MATURITY"
                result = await search_customers_by_bond_maturity(arguments)
                print(f"[MCP_ENDPOINT] search_customers_by_bond_maturity returned: {type(result)}")
            elif tool_name == "get_customer_holdings":
                print(f"[MCP_ENDPOINT] Calling get_customer_holdings")
                progress = "EXECUTING_GET_CUSTOMER_HOLDINGS"
                result = await get_customer_holdings(arguments)
                print(f"[MCP_ENDPOINT] get_customer_holdings returned: {type(result)}")
            else:
                return MCPResponse(
                    id=request.id,
                    error=f"Unknown tool: {tool_name}"
                )
            
            # 統一処理: MCPResponseかどうかで分岐
            print(f"[MCP_ENDPOINT] Tool result type: {type(result)}")
            print(f"[MCP_ENDPOINT] Tool result type: {type(result)}")
            print(f"[MCP_ENDPOINT] Tool result: {result}")
            
            if isinstance(result, MCPResponse):
                print(f"[MCP_ENDPOINT] MCPResponse detected")
                print(f"[MCP_ENDPOINT] MCPResponse.debug_response: {result.debug_response}")
                result.id = request.id
                return result
            else:
                print(f"[MCP_ENDPOINT] Non-MCPResponse detected, wrapping...")
                return MCPResponse(
                    id=request.id,
                    result=result
                )
            
        elif method == "tools/list":
            # ツール一覧
            result = await list_available_tools()
            return MCPResponse(
                id=request.id,
                result=result
            )
            
        else:
            return MCPResponse(
                id=request.id,
                error=f"Unknown method: {method}"
            )
            
    except Exception as e:
        print(f"[MCP_ENDPOINT] EXCEPTION CAUGHT!")
        print(f"[MCP_ENDPOINT] Request ID: {request.id}")
        print(f"[MCP_ENDPOINT] Progress: {progress}")
        print(f"[MCP_ENDPOINT] Method: {method}")
        print(f"[MCP_ENDPOINT] Params: {params}")
        print(f"[MCP_ENDPOINT] Tool name: {tool_name}")
        print(f"[MCP_ENDPOINT] Arguments: {arguments}")
        print(f"[MCP_ENDPOINT] Exception type: {type(e)}")
        print(f"[MCP_ENDPOINT] Exception message: {str(e)}")
        import traceback
        print(f"[MCP_ENDPOINT] Traceback: {traceback.format_exc()}")
        
        print(f"[CUSTOM_MCP_ERROR] Request ID {request.id} - Exception in mcp_endpoint at {progress}: {e}")
        return MCPResponse(
            id=request.id,
            error=str(e)
        )

@app.get("/tools/descriptions")
async def get_tool_descriptions():
    """AIChat用ツール情報（2ツール・text_input対応）"""
    return {
        "tools": [
            {
                "name": "search_customers_by_bond_maturity",
                "description": "債券の満期日条件で顧客を検索",
                "usage_context": "満期が近い債券保有顧客を調べたい、特定期間内に満期を迎える債券の顧客を探したい時に使用",
                "parameters": {
                    "text_input": {"type": "string", "description": "満期条件のテキスト（例：2年以内、6ヶ月以内）"}
                }
            },
            {
                "name": "get_customer_holdings",
                "description": "顧客の保有商品情報を取得",
                "usage_context": "特定の顧客が何を保有しているか知りたい、顧客のポートフォリオを確認したい時に使用",
                "parameters": {
                    "text_input": {"type": "string", "description": "顧客指定のテキスト（顧客ID、顧客名など）"}
                }
            }
        ]
    }

@app.get("/tools")
async def list_available_tools():
    """MCPプロトコル準拠のツール一覧（2ツール）"""
    return {
        "tools": [
            {
                "name": "search_customers_by_bond_maturity",
                "description": "債券の満期日条件で顧客を検索します（満期の近い債券保有者など）",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text_input": {"type": "string", "description": "満期条件のテキスト（例：2年以内、6ヶ月以内）"}
                    },
                    "required": ["text_input"]
                }
            },
            {
                "name": "get_customer_holdings",
                "description": "顧客の保有商品情報を取得します",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text_input": {"type": "string", "description": "顧客指定のテキスト（顧客ID、顧客名など）"}
                    },
                    "required": ["text_input"]
                }
            }
        ]
    }

async def get_customer_holdings(params: Dict[str, Any]):
    """顧客保有商品取得（text_input対応・LLM正規化）"""
    text_input = params.get("text_input", "")
    if not text_input:
        raise HTTPException(status_code=400, detail="text_input is required")
    
    # LLM正規化処理
    normalized_params = await standardize_customer_holdings_arguments(text_input)
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # 正規化されたパラメータでSQL構築
    customer_ids = normalized_params.get("customer_ids", [])
    product_type_filter = normalized_params.get("product_type_filter", "")
    
    if not customer_ids:
        return {
            "success": False,
            "error": "顧客IDが特定できませんでした",
            "normalized_params": normalized_params
        }
    
    # OR条件でSQL構築
    placeholders = ",".join(["%s"] * len(customer_ids))
    query = f"""
    SELECT h.holding_id, h.quantity, h.unit_price, h.current_price, h.current_value,
           h.purchase_date, h.customer_id,
           p.product_code, p.product_name, p.product_type, p.currency
    FROM holdings h
    JOIN products p ON h.product_code = p.product_code
    WHERE h.customer_id IN ({placeholders})
    """
    
    query_params = customer_ids
    
    # 商品タイプフィルター追加
    if product_type_filter:
        query += " AND p.product_type ILIKE %s"
        query_params.append(f"%{product_type_filter}%")
    
    query += " ORDER BY h.customer_id, p.product_type, h.current_value DESC"
    
    cursor.execute(query, query_params)
    holdings = cursor.fetchall()
    conn.close()
    
    result = []
    for holding in holdings:
        result.append({
            "id": holding['holding_id'],
            "product_code": holding['product_code'],
            "quantity": float(holding['quantity']) if holding['quantity'] else 0,
            "purchase_price": float(holding['unit_price']) if holding['unit_price'] else 0,
            "current_value": float(holding['current_value']) if holding['current_value'] else 0,
            "purchase_date": holding['purchase_date'].isoformat() if holding['purchase_date'] else None,
            "maturity_date": holding['maturity_date'].isoformat() if holding['maturity_date'] else None,
            "product_name": holding['product_name'],
            "product_type": holding['product_type'],
            "status": holding['status']
        })
    
    return MCPResponse(result=result)

async def search_customers_by_bond_maturityasync def search_customers_by_bond_maturity(params: Dict[str, Any]):
    """債券満期日条件での顧客検索"""
    import time
    start_time = time.time()
    
    print(f"[search_customers_by_bond_maturity] === FUNCTION START ===")
    print(f"[search_customers_by_bond_maturity] Received raw params: {params}")
    print(f"[search_customers_by_bond_maturity] Params type: {type(params)}")
    
    # 引数標準化処理（不定形 → 標準形式）
    standardized_params, full_prompt_text = await standardize_bond_maturity_arguments(str(params))
    print(f"[search_customers_by_bond_maturity] Standardized params: {standardized_params}")
    print(f"[search_customers_by_bond_maturity] Full prompt text: {full_prompt_text[:200]}...")
    
    days_until_maturity = standardized_params.get("days_until_maturity")
    maturity_date_from = standardized_params.get("maturity_date_from")
    maturity_date_to = standardized_params.get("maturity_date_to")
    
    print(f"[search_customers_by_bond_maturity] Extracted values:")
    print(f"  - days_until_maturity: {days_until_maturity} (type: {type(days_until_maturity)})")
    print(f"  - maturity_date_from: {maturity_date_from} (type: {type(maturity_date_from)})")
    print(f"  - maturity_date_to: {maturity_date_to} (type: {type(maturity_date_to)})")
    
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
    
    print(f"[search_customers_by_bond_maturity] Base query: {query}")
    
    if days_until_maturity:
        # SQLインジェクション対策: 文字列結合ではなくパラメータ化
        query += f" AND h.maturity_date <= CURRENT_DATE + INTERVAL '{days_until_maturity} days'"
        print(f"[search_customers_by_bond_maturity] Added days_until_maturity condition: {days_until_maturity}")
    
    if maturity_date_from:
        query += " AND h.maturity_date >= %s"
        query_params.append(maturity_date_from)
        print(f"[search_customers_by_bond_maturity] Added maturity_date_from condition: {maturity_date_from}")
    
    if maturity_date_to:
        query += " AND h.maturity_date <= %s"
        query_params.append(maturity_date_to)
        print(f"[search_customers_by_bond_maturity] Added maturity_date_to condition: {maturity_date_to}")
    
    query += " ORDER BY h.maturity_date ASC"
    
    print(f"[search_customers_by_bond_maturity] Final query: {query}")
    print(f"[search_customers_by_bond_maturity] Final query_params: {query_params}")
    
    # SQL実行（空パラメータ対策）
    if query_params:
        cursor.execute(query, query_params)
    else:
        cursor.execute(query)
    
    results = cursor.fetchall()
    
    # デバッグ: 実際のクエリ結果を詳細ログ
    print(f"[search_customers_by_bond_maturity] === DATABASE DEBUG ===")
    print(f"[search_customers_by_bond_maturity] Raw SQL executed: {query}")
    print(f"[search_customers_by_bond_maturity] Query parameters: {query_params}")
    print(f"[search_customers_by_bond_maturity] Query parameters type: {type(query_params)}")
    print(f"[search_customers_by_bond_maturity] Query parameters length: {len(query_params)}")
    
    # cursor.mogrifyのエラー詳細調査
    try:
        if query_params:
            executed_query = cursor.mogrify(query, query_params).decode('utf-8')
            print(f"[search_customers_by_bond_maturity] mogrify with params success: {executed_query}")
        else:
            # 空パラメータの場合の対処
            executed_query = cursor.mogrify(query).decode('utf-8')
            print(f"[search_customers_by_bond_maturity] mogrify without params success: {executed_query}")
    except Exception as mogrify_error:
        print(f"[search_customers_by_bond_maturity] mogrify error: {mogrify_error}")
        print(f"[search_customers_by_bond_maturity] mogrify error type: {type(mogrify_error)}")
        executed_query = f"MOGRIFY_ERROR: {query} (params: {query_params})"
    
    print(f"[search_customers_by_bond_maturity] Final executed_query: {executed_query}")
    print(f"[search_customers_by_bond_maturity] Raw results count: {len(results)}")
    print(f"[search_customers_by_bond_maturity] === DATABASE DEBUG END ===")
    
    conn.close()
    
    print(f"[search_customers_by_bond_maturity] Query executed, found {len(results)} rows")
    
    execution_time = time.time() - start_time
    
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
    execution_time = time.time() - start_time
    
    # debug_response作成（必要な情報のみ）
    tool_debug = {
        "executed_query": query,
        "standardize_prompt": full_prompt_text,
        "execution_time_ms": round(execution_time * 1000, 2),
        "results_count": len(customers)
    }
    
    print(f"[search_customers_by_bond_maturity] Returning result with {len(customers)} customers")
    print(f"[search_customers_by_bond_maturity] === FUNCTION END ===")
    
    return MCPResponse(result=customers, debug_response=tool_debug)

async def standardize_bond_maturity_arguments(raw_input: str) -> tuple[Dict[str, Any], str]:
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

    # 合成プロンプトテキスト作成
    full_prompt_text = f"{system_prompt}\n\nUser Input: {raw_input}"

    print(f"[standardize_bond_maturity_arguments] === LLM CALL START ===")
    print(f"[standardize_bond_maturity_arguments] System Prompt:")
    print(f"{system_prompt}")
    print(f"[standardize_bond_maturity_arguments] User Input: {raw_input}")
    
    # LLM呼び出し（エラー時はそのまま例外を投げる）
    import boto3
    import json as json_lib
    
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
    
    body = json_lib.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
        "system": system_prompt,
        "messages": [{"role": "user", "content": raw_input}]
    })
    
    print(f"[standardize_bond_maturity_arguments] Bedrock Request Body:")
    print(f"{body}")
    
    response = bedrock.invoke_model(
        body=body,
        modelId='anthropic.claude-3-sonnet-20240229-v1:0',
        accept='application/json',
        contentType='application/json'
    )
    
    response_body = json_lib.loads(response.get('body').read())
    standardized_text = response_body['content'][0]['text']
    
    print(f"[standardize_bond_maturity_arguments] LLM Raw Response:")
    print(f"{standardized_text}")
    print(f"[standardize_bond_maturity_arguments] === LLM CALL END ===")
    
    # JSON部分を抽出
    import re
    json_match = re.search(r'\{.*\}', standardized_text, re.DOTALL)
    if not json_match:
        print(f"[standardize_bond_maturity_arguments] ERROR: No JSON found in response")
        raise ValueError(f"LLM response does not contain valid JSON: {standardized_text}")
    
    extracted_json = json_match.group()
    print(f"[standardize_bond_maturity_arguments] Extracted JSON: {extracted_json}")
    
    standardized = json_lib.loads(extracted_json)
    print(f"[standardize_bond_maturity_arguments] Final Standardized Output: {standardized}")
    return standardized, full_prompt_text

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
