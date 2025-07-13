# 🔐 Security Setup Guide

## ⚠️ IMPORTANT: API Token Security

This application requires secure handling of API keys and tokens. **Never commit secrets to Git!**

## 🛠️ Setup Instructions

### 1. Create Streamlit Secrets File

1. Create the `.streamlit` directory if it doesn't exist:
   ```bash
   mkdir -p .streamlit
   ```

2. Copy the template to create your secrets file:
   ```bash
   cp .secrets.toml.template .streamlit/secrets.toml
   ```

3. Edit `.streamlit/secrets.toml` with your actual API credentials:
   ```toml
   [voxuy]
   api_token = "your_actual_voxuy_token_here"
   
   [google_sheets]
   # Your Google Sheets API credentials here
   
   [auth_users]
   # Your user credentials here
   ```

### 2. Alternative: Environment Variables

Set the environment variable:
```bash
export VOXUY_API_TOKEN="your_actual_voxuy_token_here"
```

### 3. Verify Setup

The application will warn you if secrets are not properly configured.

## 🔒 Security Best Practices

- ✅ Secrets are loaded from environment variables or `.streamlit/secrets.toml`
- ✅ Secrets files are in `.gitignore`
- ✅ Template file shows structure without exposing actual secrets
- ❌ Never commit actual API tokens to Git
- ❌ Never share secrets in chat/email

## 📁 Files Protected by .gitignore

- `.streamlit/secrets.toml`
- `credentials.json`
- `.env` files
- Any `.key` or `.pem` files
- `voxuy_token.txt`
- `api_keys.txt`

## 🔧 For Production Deployment

When deploying to Streamlit Cloud or other platforms:

1. Use the platform's secrets management system
2. Never include secrets in your repository
3. Set environment variables in your deployment platform
4. Rotate API keys regularly

## 🚨 If Secrets Were Exposed

1. **Immediately revoke/regenerate** the exposed API keys
2. Update your secrets with new keys
3. Monitor for unauthorized usage
4. Review Git history and clean if necessary