# LLM Client utilities

import boto3
import json
from config import BEDROCK_CONFIG

class BedrockClient:
    def __init__(self):
        self.bedrock = boto3.client('bedrock-runtime', region_name=BEDROCK_CONFIG["region_name"])
        self.model_id = BEDROCK_CONFIG["model_id"]
    
    async def call_claude(self, system_prompt: str, user_message: str = "") -> str:
        """Claude 3 Sonnet 呼び出し"""
        try:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_message}]
            }
            
            response = self.bedrock.invoke_model(
                body=json.dumps(body),
                modelId=self.model_id
            )
            
            response_body = json.loads(response.get('body').read())
            return response_body['content'][0]['text']
            
        except Exception as e:
            print(f"[ERROR] Bedrock call failed: {e}")
            return f"LLMエラー: {str(e)}"

# グローバルインスタンス
bedrock_client = BedrockClient()
