import httpx

async def get_system_prompt(prompt_key: str) -> str:
    """SystemPrompt Management APIからプロンプト取得"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"http://localhost:8002/api/system-prompts/{prompt_key}",
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                return data["prompt_text"]
            else:
                return f"システムプロンプトが取得できませんでした (key: {prompt_key}, status: {response.status_code})"
                
    except Exception as e:
        return f"システムプロンプトが取得できませんでした (key: {prompt_key}, error: {str(e)})"
