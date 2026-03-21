# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in OpenArg, please report it responsibly:

1. **Do NOT open a public GitHub issue**
2. Email: admin@colossuslab.org
3. Include: description, steps to reproduce, potential impact

We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Deployment Security Checklist

Before deploying OpenArg to production, ensure:

- [ ] All secrets in `.env` are generated fresh (never use defaults)
- [ ] `POSTGRES_PASSWORD` is a strong random string (32+ chars)
- [ ] `REDIS_PASSWORD` is a strong random string
- [ ] `NEXTAUTH_SECRET` is generated with `openssl rand -base64 32`
- [ ] `BACKEND_API_KEY` is generated with `openssl rand -base64 24`
- [ ] Google OAuth credentials are configured for your domain
- [ ] `CORS_ALLOWED_ORIGINS` matches your frontend domain only
- [ ] `SANDBOX_DATABASE_URL` points to a read-only database user
- [ ] `APP_ENV=prod` is set (disables docs endpoints)
- [ ] Caddy/reverse proxy has HTTPS enabled

## Architecture Security Notes

- **SQL Sandbox**: Queries are validated through 3 layers (regex, table allowlist, AST parsing) and executed in read-only transactions with a 10-second timeout
- **Prompt Injection**: Detected via regex patterns + keyword scoring (threshold 0.6). Consider adding LLM-based detection as a secondary layer
- **Rate Limiting**: Backend uses SlowAPI on critical endpoints. Frontend has in-memory per-user rate limiting
- **Authentication**: API key validated with constant-time comparison (`secrets.compare_digest`)
