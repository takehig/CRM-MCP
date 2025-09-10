# CRM-MCP Configuration

import os

# データベース設定
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "wealthai",
    "user": "wealthai_user",
    "password": "wealthai123"  # CRM/.env から取得した正しいパスワード
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
