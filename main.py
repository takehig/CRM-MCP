# CRM-MCP Main Application

from fastapi import FastAPI, HTTPException
from datetime import datetime
from models import MCPRequest, MCPResponse
from tools.bond_maturity import search_customers_by_bond_maturity
from tools.customer_holdings import get_customer_holdings
from config import MCP_CONFIG

app = FastAPI(title="CRM-MCP Server", version=MCP_CONFIG["version"])

@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    return {
        "status": "healthy", 
        "service": MCP_CONFIG["server_name"], 
        "timestamp": datetime.now().isoformat()
    }

@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCPプロトコルエンドポイント"""
    print(f"[MCP_ENDPOINT] === REQUEST START ===")
    print(f"[MCP_ENDPOINT] Request received: {request}")
    
    try:
        method = request.method
        params = request.params
        
        if method == "initialize":
            # 初期化
            return MCPResponse(
                id=request.id,
                result={
                    "protocolVersion": MCP_CONFIG["protocol_version"],
                    "capabilities": {},
                    "serverInfo": {
                        "name": MCP_CONFIG["server_name"],
                        "version": MCP_CONFIG["version"]
                    }
                }
            )
        
        elif method == "tools/list":
            # ツール一覧
            return MCPResponse(
                id=request.id,
                result={
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
            )
        
        elif method == "tools/call":
            # ツール実行
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            print(f"[MCP_ENDPOINT] Tool name: {tool_name}")
            print(f"[MCP_ENDPOINT] Arguments: {arguments}")
            
            if tool_name == "search_customers_by_bond_maturity":
                print(f"[MCP_ENDPOINT] Calling search_customers_by_bond_maturity")
                result = await search_customers_by_bond_maturity(arguments)
                result.id = request.id
                return result
                
            elif tool_name == "get_customer_holdings":
                print(f"[MCP_ENDPOINT] Calling get_customer_holdings")
                result = await get_customer_holdings(arguments)
                result.id = request.id
                return result
                
            else:
                return MCPResponse(
                    id=request.id,
                    error=f"Unknown tool: {tool_name}"
                )
        
        else:
            return MCPResponse(
                id=request.id,
                error=f"Unknown method: {method}"
            )
    
    except Exception as e:
        print(f"[MCP_ENDPOINT] EXCEPTION CAUGHT!")
        print(f"[MCP_ENDPOINT] Exception: {e}")
        
        return MCPResponse(
            id=request.id,
            result=f"サーバーエラー: {str(e)}",
            debug_response={
                "error": str(e),
                "error_type": type(e).__name__,
                "method": method,
                "params": params
            }
        )

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

@app.get("/tools/descriptions")
async def get_tool_descriptions():
    """ツール詳細情報（AIChat用）"""
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
