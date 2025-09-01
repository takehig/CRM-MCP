#!/usr/bin/env python3
"""
CRM MCP Server - 顧客情報・営業データ検索API
Port: 8004
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import os

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

# データベース接続
CRM_DB_PATH = "/home/ec2-user/wealthai/data/wealthai.db"

class MCPRequest(BaseModel):
    method: str
    params: Dict[str, Any] = {}

class MCPResponse(BaseModel):
    result: Any
    error: Optional[str] = None

def get_db_connection():
    """CRMデータベース接続"""
    if not os.path.exists(CRM_DB_PATH):
        raise HTTPException(status_code=500, detail="CRM database not found")
    return sqlite3.connect(CRM_DB_PATH)

@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    return {"status": "healthy", "service": "CRM-MCP", "timestamp": datetime.now().isoformat()}

@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCP統一エンドポイント"""
    try:
        method = request.method
        params = request.params
        
        if method == "search_customers":
            return await search_customers(params)
        elif method == "get_customer_holdings":
            return await get_customer_holdings(params)
        elif method == "search_sales_notes":
            return await search_sales_notes(params)
        elif method == "get_cash_inflows":
            return await get_cash_inflows(params)
        elif method == "list_tools":
            return await list_available_tools()
        else:
            raise HTTPException(status_code=400, detail=f"Unknown method: {method}")
            
    except Exception as e:
        logger.error(f"MCP error: {e}")
        return MCPResponse(result=None, error=str(e))

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
            }
        ]
    }
async def search_customers(params: Dict[str, Any]):
    """顧客検索"""
    name = params.get("name", "")
    risk_level = params.get("risk_level")
    limit = params.get("limit", 10)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT id, name, email, phone, risk_level, total_assets FROM customers WHERE 1=1"
    query_params = []
    
    if name:
        query += " AND name LIKE ?"
        query_params.append(f"%{name}%")
    
    if risk_level:
        query += " AND risk_level = ?"
        query_params.append(risk_level)
    
    query += f" LIMIT {limit}"
    
    cursor.execute(query, query_params)
    customers = cursor.fetchall()
    conn.close()
    
    result = []
    for customer in customers:
        result.append({
            "id": customer[0],
            "name": customer[1],
            "email": customer[2],
            "phone": customer[3],
            "risk_level": customer[4],
            "total_assets": customer[5]
        })
    
    return MCPResponse(result=result)

async def get_customer_holdings(params: Dict[str, Any]):
    """顧客保有商品取得"""
    customer_id = params.get("customer_id")
    if not customer_id:
        raise HTTPException(status_code=400, detail="customer_id is required")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT h.id, h.product_code, h.quantity, h.purchase_price, h.current_value,
           h.purchase_date, p.product_name, p.product_type
    FROM holdings h
    LEFT JOIN products p ON h.product_code = p.product_code
    WHERE h.customer_id = ?
    """
    
    cursor.execute(query, (customer_id,))
    holdings = cursor.fetchall()
    conn.close()
    
    result = []
    for holding in holdings:
        result.append({
            "id": holding[0],
            "product_code": holding[1],
            "quantity": holding[2],
            "purchase_price": holding[3],
            "current_value": holding[4],
            "purchase_date": holding[5],
            "product_name": holding[6],
            "product_type": holding[7]
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
