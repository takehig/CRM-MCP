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
import psycopg2
from psycopg2.extras import RealDictCursor

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
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    return {"status": "healthy", "service": "CRM-MCP", "timestamp": datetime.now().isoformat()}

@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """標準MCPプロトコル対応エンドポイント"""
    try:
        method = request.method
        params = request.params
        
        if method == "tools/call":
            # 標準MCPプロトコル: tools/call
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            if tool_name == "search_customers":
                result = await search_customers(arguments)
            elif tool_name == "get_customer_holdings":
                result = await get_customer_holdings(arguments)
            elif tool_name == "search_customers_by_bond_maturity":
                result = await search_customers_by_bond_maturity(arguments)
            elif tool_name == "search_sales_notes":
                result = await search_sales_notes(arguments)
            elif tool_name == "get_cash_inflows":
                result = await get_cash_inflows(arguments)
            else:
                return MCPResponse(
                    id=request.id,
                    error=f"Unknown tool: {tool_name}"
                )
            
            # 統一処理: MCPResponseかどうかで分岐
            print(f"[MCP_ENDPOINT] Tool result type: {type(result)}")
            print(f"[MCP_ENDPOINT] Tool result: {result}")
            logger.info(f"[DEBUG] Tool result type: {type(result)}")
            logger.info(f"[DEBUG] Tool result: {result}")
            
            if isinstance(result, MCPResponse):
                print(f"[MCP_ENDPOINT] MCPResponse detected")
                print(f"[MCP_ENDPOINT] MCPResponse.debug_response: {result.debug_response}")
                logger.info(f"[DEBUG] MCPResponse detected")
                logger.info(f"[DEBUG] MCPResponse.debug_response: {result.debug_response}")
                result.id = request.id
                return result
            else:
                print(f"[MCP_ENDPOINT] Non-MCPResponse detected, wrapping...")
                logger.info(f"[DEBUG] Non-MCPResponse detected, wrapping...")
                return MCPResponse(
                    id=request.id,
                    result=result
                )
            
        elif method == "tools/list":
            # ツール一覧
            result = await list_available_tools()
            return MCPResponse(
                id=request.id,
                result=result.result
            )
            
        else:
            return MCPResponse(
                id=request.id,
                error=f"Unknown method: {method}"
            )
            
    except Exception as e:
        logger.error(f"MCP error: {e}")
        return MCPResponse(
            id=request.id,
            error=str(e)
        )

@app.get("/tools")
async def list_available_tools():
    """MCPプロトコル準拠のツール一覧"""
    return {
        "tools": [
            {
                "name": "search_customers",
                "description": "顧客情報を検索します",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "検索キーワード"}
                    }
                }
            },
            {
                "name": "get_customer_holdings",
                "description": "顧客の保有商品情報を取得します",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "customer_id": {"type": "string", "description": "顧客ID"}
                    },
                    "required": ["customer_id"]
                }
            },
            {
                "name": "search_customers_by_bond_maturity",
                "description": "債券の満期日条件で顧客を検索します（満期の近い債券保有者など）",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "days_until_maturity": {"type": "integer", "description": "満期までの日数（例：90日以内なら90）"},
                        "maturity_date_from": {"type": "string", "description": "満期日開始（YYYY-MM-DD形式）"},
                        "maturity_date_to": {"type": "string", "description": "満期日終了（YYYY-MM-DD形式）"}
                    }
                }
            },
            {
                "name": "search_customers_by_maturity",
                "description": "満期日条件で顧客を検索します（満期の近い債券保有者など）",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "days_until_maturity": {"type": "integer", "description": "満期までの日数（例：90日以内）"},
                        "maturity_date_from": {"type": "string", "description": "満期日開始（YYYY-MM-DD形式）"},
                        "maturity_date_to": {"type": "string", "description": "満期日終了（YYYY-MM-DD形式）"},
                        "product_type": {"type": "string", "description": "商品タイプ（債券など）"}
                    }
                }
            }
        ]
    }
async def search_customers(params: Dict[str, Any]):
    """顧客検索"""
    name = params.get("name", "")
    risk_level = params.get("risk_level")
    limit = params.get("limit", 10)
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT customer_id, name, email, phone, risk_tolerance, net_worth FROM customers WHERE 1=1"
    query_params = []
    
    if name:
        query += " AND name ILIKE %s"
        query_params.append(f"%{name}%")
    
    if risk_level:
        query += " AND risk_tolerance = %s"
        query_params.append(risk_level)
    
    query += f" LIMIT {limit}"
    
    cursor.execute(query, query_params)
    customers = cursor.fetchall()
    conn.close()
    
    result = []
    for customer in customers:
        result.append({
            "id": customer['customer_id'],
            "name": customer['name'],
            "email": customer['email'],
            "phone": customer['phone'],
            "risk_level": customer['risk_tolerance'],
            "total_assets": customer['net_worth']
        })
    
    return MCPResponse(result=result)

async def get_customer_holdings(params: Dict[str, Any]):
    """顧客保有商品取得"""
    customer_id = params.get("customer_id")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customer_id is required")
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT h.holding_id, h.quantity, h.unit_price, h.current_price, h.current_value,
           h.purchase_date, h.maturity_date, h.status,
           p.product_code, p.product_name, p.product_type, p.maturity_date as product_maturity
    FROM holdings h
    LEFT JOIN products p ON h.product_id = p.product_id
    WHERE h.customer_id = %s
    """
    
    cursor.execute(query, (customer_id,))
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

async def search_sales_notes(params: Dict[str, Any]):
    """営業メモ検索"""
    customer_id = params.get("customer_id")
    keyword = params.get("keyword", "")
    limit = params.get("limit", 10)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT id, customer_id, note_date, content, priority FROM sales_notes WHERE 1=1"
    query_params = []
    
    if customer_id:
        query += " AND customer_id = ?"
        query_params.append(customer_id)
    
    if keyword:
        query += " AND content LIKE ?"
        query_params.append(f"%{keyword}%")
    
    query += f" ORDER BY note_date DESC LIMIT {limit}"
    
    cursor.execute(query, query_params)
    notes = cursor.fetchall()
    conn.close()
    
    result = []
    for note in notes:
        result.append({
            "id": note[0],
            "customer_id": note[1],
            "note_date": note[2],
            "content": note[3],
            "priority": note[4]
        })
    
    return MCPResponse(result=result)

async def get_cash_inflows(params: Dict[str, Any]):
    """入金予測取得"""
    customer_id = params.get("customer_id")
    date_from = params.get("date_from")
    date_to = params.get("date_to")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT id, customer_id, expected_date, amount, source, confidence FROM cash_inflows WHERE 1=1"
    query_params = []
    
    if customer_id:
        query += " AND customer_id = ?"
        query_params.append(customer_id)
    
    if date_from:
        query += " AND expected_date >= ?"
        query_params.append(date_from)
    
    if date_to:
        query += " AND expected_date <= ?"
        query_params.append(date_to)
    
    query += " ORDER BY expected_date"
    
    cursor.execute(query, query_params)
    inflows = cursor.fetchall()
    conn.close()
    
    result = []
    for inflow in inflows:
        result.append({
            "id": inflow[0],
            "customer_id": inflow[1],
            "expected_date": inflow[2],
            "amount": inflow[3],
            "source": inflow[4],
            "confidence": inflow[5]
        })
    
    return MCPResponse(result=result)

async def search_customers_by_bond_maturity(params: Dict[str, Any]):
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
        query += " AND h.maturity_date <= CURRENT_DATE + INTERVAL '%s days'"
        query_params.append(days_until_maturity)
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
    
    cursor.execute(query, query_params)
    results = cursor.fetchall()
    
    # デバッグ: 実際のクエリ結果を詳細ログ
    print(f"[search_customers_by_bond_maturity] === DATABASE DEBUG ===")
    print(f"[search_customers_by_bond_maturity] Raw SQL executed:")
    executed_query = cursor.mogrify(query, query_params).decode('utf-8')
    print(f"{executed_query}")
    print(f"[search_customers_by_bond_maturity] Query parameters: {query_params}")
    print(f"[search_customers_by_bond_maturity] Raw results count: {len(results)}")
    print(f"[search_customers_by_bond_maturity] Executed query (formatted):")
    print(f"  Query: {query}")
    print(f"  Params: {query_params}")
    print(f"  Full SQL: {executed_query}")
    if len(results) > 0:
        print(f"[search_customers_by_bond_maturity] Sample result: {dict(results[0])}")
    
    # 基本データ確認
    cursor.execute("SELECT COUNT(*) FROM products WHERE product_type ILIKE %s OR product_type ILIKE %s", ('%債券%', '%bond%'))
    bond_products_count = cursor.fetchone()[0]
    print(f"[search_customers_by_bond_maturity] Total bond products in DB: {bond_products_count}")
    
    cursor.execute("SELECT COUNT(*) FROM holdings h JOIN products p ON h.product_id = p.product_id WHERE p.product_type ILIKE %s OR p.product_type ILIKE %s", ('%債券%', '%bond%'))
    bond_holdings_count = cursor.fetchone()[0]
    print(f"[search_customers_by_bond_maturity] Total bond holdings in DB: {bond_holdings_count}")
    
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
    
    # tool_debug情報作成
    tool_debug = {
        "executed_query": executed_query,
        "query_parameters": query_params,
        "standardized_params": standardized_params,
        "standardize_prompt": full_prompt_text,
        "execution_time_ms": round(execution_time * 1000, 2),
        "results_count": len(customers)
    }
    
    print(f"[search_customers_by_bond_maturity] Returning result with {len(customers)} customers")
    print(f"[search_customers_by_bond_maturity] tool_debug: {tool_debug}")
    print(f"[search_customers_by_bond_maturity] === FUNCTION END ===")
    
    mcp_response = MCPResponse(result=customers, debug_response=tool_debug)
    print(f"[search_customers_by_bond_maturity] Created MCPResponse: {mcp_response}")
    print(f"[search_customers_by_bond_maturity] MCPResponse.debug_response: {mcp_response.debug_response}")
    
    return mcp_response

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
