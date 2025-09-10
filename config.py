# CRM-MCP Configuration

import os
from dotenv import load_dotenv

# .env ファイル読み込み
load_dotenv()

# データベース設定（.env ファイルから取得）
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "wealthai"),
    "user": os.getenv("DB_USER", "wealthai_user"),
    "password": os.getenv("DB_PASSWORD", "wealthai123")
}

# Bedrock設定
BEDROCK_CONFIG = {
    "region_name": "us-east-1",
    "model_id": "anthropic.claude-3-sonnet-20240229-v1:0"
}

# MCP設定
MCP_CONFIG = {
    "server_name": "CRM-MCP",
    "version": "1.0.0",
    "protocol_version": "2024-11-05"
}
