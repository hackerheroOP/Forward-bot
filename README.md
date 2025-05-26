# Telegram Forward Bot with Web Dashboard

[![Deploy to Koyeb](https://www.koyeb.com/static/images/deploy/button.svg)](https://app.koyeb.com/deploy?type=git&repository=YOUR_REPO_URL)

## Features
- Message forwarding with fixed/random intervals
- Web dashboard on port 8080
- MongoDB persistence
- Admin broadcast
- Bulk channel forwarding

## Deployment
1. Create `.env` using `.env.example` template
2. Deploy to Koyeb with GitHub/GitLab repository
3. Koyeb will automatically:
   - Install dependencies from `requirements.txt`
   - Start FastAPI web server on port 8080
   - Run Telegram bot as background worker

## Local Development
