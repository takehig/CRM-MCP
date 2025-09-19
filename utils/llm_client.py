"""
LLM Client for CRM MCP
Amazon Bedrock Claude 3 Sonnet を使用したLLM呼び出し
"""

import boto3
import json
import asyncio
from typing import Optional

# Bedrock クライアント初期化
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')

async def call_bedrock_llm(system_prompt: str, user_input: str) -> str:
    """
    Amazon Bedrock Claude 3 Sonnet を使用してLLM呼び出し
    
    Args:
        system_prompt: システムプロンプト
        user_input: ユーザー入力
        
    Returns:
        str: LLMレスポンス
    """
    try:
        # Claude 3 Sonnet用のリクエスト構築
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4000,
            "system": system_prompt,
            "messages": [
                {
                    "role": "user",
                    "content": user_input
                }
            ]
        }
        
        # 同期呼び出しを非同期で実行
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: bedrock_client.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps(request_body),
                contentType="application/json"
            )
        )
        
        # レスポンス解析
        response_body = json.loads(response['body'].read())
        return response_body['content'][0]['text']
        
    except Exception as e:
        return f"LLM呼び出しエラー: {str(e)}"
