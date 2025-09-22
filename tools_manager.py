import httpx
from typing import Dict, List, Any, Optional

class ToolsManager:
    """ツール定義の一元管理クラス - MCP-Management API対応・直接呼び出し設計"""
    
    def __init__(self, mcp_management_url: str = "http://localhost:8008"):
        self.mcp_management_url = mcp_management_url
    
    async def get_tools_from_management(self) -> List[Dict[str, Any]]:
        """MCP-Management から CRM MCP のツール一覧を取得"""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.mcp_management_url}/api/tools")
                if response.status_code == 200:
                    data = response.json()
                    print(f"[ToolsManager] MCP-Management response: {data}")
                    
                    # レスポンス構造確認・修正
                    if isinstance(data, list):
                        # 直接リストの場合
                        tools_list = data
                    elif isinstance(data, dict) and "tools" in data:
                        # {"tools": [...]} の場合
                        tools_list = data["tools"]
                    else:
                        print(f"[ToolsManager] Unexpected response structure: {data}")
                        return []
                    
                    # CRM MCP のツールのみ返す (enabled フィルタリング削除)
                    crm_tools = [tool for tool in tools_list if tool.get("mcp_server_name") == "CRM MCP"]
                    print(f"[ToolsManager] CRM MCP tools: {[tool.get('tool_key') for tool in crm_tools]}")
                    return crm_tools
                else:
                    print(f"[ToolsManager] MCP-Management API error: {response.status_code}")
                    return []
        except Exception as e:
            print(f"[ToolsManager] Failed to fetch tools from MCP-Management: {e}")
            return []
    
    async def get_tools_list(self) -> List[Dict[str, Any]]:
        """tools/list用のツール一覧"""
        tools = await self.get_tools_from_management()
        
        return [
            {
                "name": tool["tool_key"],  # tool_key をそのまま name として使用
                "description": tool["description"],
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text_input": {
                            "type": "string",
                            "description": "入力テキスト"
                        }
                    },
                    "required": ["text_input"]
                }
            }
            for tool in tools
        ]
    
    async def get_tools_descriptions(self) -> List[Dict[str, Any]]:
        """/tools/descriptions用の詳細情報"""
        tools = await self.get_tools_from_management()
        
        return [
            {
                "name": tool["tool_key"],
                "description": tool["description"],
                "usage_context": f"{tool['tool_name']}を使用",
                "parameters": {
                    "text_input": {
                        "type": "string",
                        "description": "入力テキスト"
                    }
                }
            }
            for tool in tools
        ]
    
    async def get_mcp_tools_format(self) -> List[Dict[str, Any]]:
        """MCPプロトコル用のツール一覧"""
        return await self.get_tools_descriptions()
    
    async def is_valid_tool(self, tool_name: str) -> bool:
        """ツール名の有効性チェック"""
        tools = await self.get_tools_from_management()
        
        print(f"[ToolsManager] is_valid_tool called with: {tool_name}")
        print(f"[ToolsManager] Available tools: {[tool.get('tool_key') for tool in tools]}")
        
        result = any(tool["tool_key"] == tool_name for tool in tools)
        print(f"[ToolsManager] is_valid_tool result: {result}")
        
        return result
    
    async def get_tool_function(self, tool_name: str):
        """ツール名から関数を直接取得 - マッピングなし"""
        # 直接インポート・呼び出し
        if tool_name == "search_customers_by_bond_maturity":
            from tools.bond_maturity import search_customers_by_bond_maturity
            return search_customers_by_bond_maturity
        elif tool_name == "get_customer_holdings":
            from tools.customer_holdings import get_customer_holdings
            return get_customer_holdings
        elif tool_name == "predict_cash_inflow_from_sales_notes":
            from tools.cash_inflow_prediction import predict_cash_inflow_from_sales_notes
            return predict_cash_inflow_from_sales_notes
        elif tool_name == "get_customers_by_product_text":
            from tools.product_customers import get_customers_by_product_text
            return get_customers_by_product_text
        else:
            print(f"[ToolsManager] Unknown tool: {tool_name}")
            return None
    
    async def get_tool_names(self) -> List[str]:
        """全ツール名のリスト"""
        tools = await self.get_tools_from_management()
        return [tool["tool_key"] for tool in tools]
