# CRM-MCP Main Application

from fastapi import FastAPI, HTTPException
from datetime import datetime
from models import MCPRequest, MCPResponse
from tools_manager import ToolsManager
from config import MCP_CONFIG

app = FastAPI(title="CRM-MCP Server", version=MCP_CONFIG["version"])

# ツール管理インスタンス
tools_manager = ToolsManager()

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
            # ツール一覧（一元管理から取得）
            return MCPResponse(
                id=request.id,
                result={
                    "tools": await tools_manager.get_mcp_tools_format()
                }
            )
        
        elif method == "tools/call":
            # ツール実行
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            print(f"[MCP_ENDPOINT] Tool name: {tool_name}")
            print(f"[MCP_ENDPOINT] Arguments: {arguments}")
            
            # 動的ツール実行
            if await tools_manager.is_valid_tool(tool_name):
                print(f"[MCP_ENDPOINT] Calling {tool_name}")
                tool_function = await tools_manager.get_tool_function(tool_name)
                
                if tool_function:
                    tool_response = await tool_function(arguments)
                    tool_response.id = request.id
                    print(f"[MCP_ENDPOINT] About to return response: {tool_response}")
                    return tool_response
                else:
                    error_msg = f"Tool function not found: {tool_name}"
                    response = MCPResponse(
                        id=request.id,
                        result=error_msg,
                        error=error_msg
                    )
                    print(f"[MCP_ENDPOINT] About to return error response: {response}")
                    return response
            else:
                error_msg = f"Unknown tool: {tool_name}"
                response = MCPResponse(
                    id=request.id,
                    result=error_msg,
                    error=error_msg
                )
                print(f"[MCP_ENDPOINT] About to return unknown tool response: {response}")
                return response
        
        else:
            error_msg = f"Unknown method: {method}"
            return MCPResponse(
                id=request.id,
                result=error_msg,
                error=error_msg
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
    """MCPプロトコル準拠のツール一覧"""
    return {
        "tools": await tools_manager.get_tools_list()
    }

@app.get("/tools/descriptions")
async def get_tool_descriptions():
    """ツール詳細情報（AIChat用）"""
    return {
        "tools": await tools_manager.get_tools_descriptions()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
