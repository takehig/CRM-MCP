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
    debug_info: Optional[Dict[str, Any]] = None

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
                return MCPResponse(
                    id=request.id,
                    result=result["result"],
                    debug_info=result["debug_info"]
                )
            elif tool_name == "search_sales_notes":
                result = await search_sales_notes(arguments)
            elif tool_name == "get_cash_inflows":
                result = await get_cash_inflows(arguments)
            else:
                return MCPResponse(
                    id=request.id,
                    error=f"Unknown tool: {tool_name}"
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
async def search_customers_by_bond_maturity(params: Dict[str, Any]):
    """債券満期日条件での顧客検索"""
    import time
    start_time = time.time()
    
    print(f"[CRM-MCP DEBUG] === search_customers_by_bond_maturity START ===")
    print(f"[CRM-MCP DEBUG] Received params: {params}")
    print(f"[CRM-MCP DEBUG] Params type: {type(params)}")
    
    days_until_maturity = params.get("days_until_maturity")
    maturity_date_from = params.get("maturity_date_from")
    maturity_date_to = params.get("maturity_date_to")
    
    print(f"[CRM-MCP DEBUG] Extracted values:")
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
    
    print(f"[CRM-MCP DEBUG] Base query: {query}")
    
    if days_until_maturity:
        query += " AND h.maturity_date <= CURRENT_DATE + INTERVAL '%s days'"
        query_params.append(days_until_maturity)
        print(f"[CRM-MCP DEBUG] Added days_until_maturity condition: {days_until_maturity}")
    
    if maturity_date_from:
        query += " AND h.maturity_date >= %s"
        query_params.append(maturity_date_from)
        print(f"[CRM-MCP DEBUG] Added maturity_date_from condition: {maturity_date_from}")
    
    if maturity_date_to:
        query += " AND h.maturity_date <= %s"
        query_params.append(maturity_date_to)
        print(f"[CRM-MCP DEBUG] Added maturity_date_to condition: {maturity_date_to}")
    
    query += " ORDER BY h.maturity_date ASC"
    
    print(f"[CRM-MCP DEBUG] Final query: {query}")
    print(f"[CRM-MCP DEBUG] Final query_params: {query_params}")
    
    cursor.execute(query, query_params)
    results = cursor.fetchall()
    conn.close()
    
    print(f"[CRM-MCP DEBUG] Query executed, found {len(results)} rows")
    
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
    
    result = {
        "result": customers,
        "debug_info": {
            "tool_name": "search_customers_by_bond_maturity",
            "executed_query": query,
            "query_params": query_params,
            "input_params": params,
            "execution_time_ms": round(execution_time * 1000, 2),
            "rows_found": len(customers),
            "timestamp": datetime.now().isoformat()
        }
    }
    
    print(f"[CRM-MCP DEBUG] Returning result with {len(customers)} customers")
    print(f"[CRM-MCP DEBUG] === search_customers_by_bond_maturity END ===")
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
