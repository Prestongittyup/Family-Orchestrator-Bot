# HPAL Frontend Deployment Guide

This guide covers deployment options for the HPAL Frontend Control Surface.

## Prerequisites

- Build artifacts in `dist/` directory (run `npm run build`)
- Environment configuration (`.env` file or platform-specific variables)
- HPAL backend accessible from deployed frontend

## Deployment Options

### Option 1: Azure Static Web Apps (Recommended for Azure Stack)

#### Setup

1. **Create Resource Group**

```bash
az group create -n rg-hpal-frontend -l eastus
```

2. **Create Static Web App**

```bash
az staticwebapp create \
  --name hpal-frontend \
  --resource-group rg-hpal-frontend \
  --location eastus \
  --sku Standard
```

3. **Link GitHub Repo** (via Azure Portal or CLI)

```bash
az staticwebapp create \
  --name hpal-frontend \
  --resource-group rg-hpal-frontend \
  --source https://github.com/YOUR_ORG/family-orchestration-bot \
  --branch main \
  --app-location "hpal-frontend" \
  --output-location "dist"
```

4. **Configure Build**

Create `.github/workflows/azure-static-web-apps-*.yml`:

```yaml
name: Azure Static Web Apps CI/CD

on:
  push:
    branches:
      - main
    paths:
      - "hpal-frontend/**"

jobs:
  build_and_deploy_job:
    runs-on: ubuntu-latest
    name: Build and Deploy Job
    steps:
      - uses: actions/checkout@v3

      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: 18

      - name: Install Dependencies
        working-directory: ./hpal-frontend
        run: npm ci

      - name: Build
        working-directory: ./hpal-frontend
        run: npm run build

      - name: Deploy to Static Web Apps
        uses: Azure/static-web-apps-deploy@v1
        with:
          azure_static_web_apps_api_token: ${{ secrets.AZURE_STATIC_WEB_APPS_API_TOKEN }}
          repo_token: ${{ secrets.GITHUB_TOKEN }}
          action: "upload"
          app_location: "./hpal-frontend/dist"
          output_location: ""
```

5. **Configure Environment Variables**

In Azure Portal → Static Web Apps → Configuration → Application Settings:

```
VITE_API_BASE_URL=https://hpal-backend.azurewebsites.net/api
```

#### Deploy

```bash
git push origin main
```

GitHub Actions builds and deploys to Azure Static Web Apps automatically.

---

### Option 2: Docker + Container Registry (Flexible Deployment)

#### Dockerfile

```dockerfile
# Build stage
FROM node:18-alpine as builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# Runtime stage
FROM nginx:alpine
WORKDIR /usr/share/nginx/html

# Copy built artifacts
COPY --from=builder /app/dist .

# Copy nginx config
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

#### nginx.conf

```nginx
events {
    worker_connections 1024;
}

http {
    server {
        listen 80;

        location / {
            root /usr/share/nginx/html;
            try_files $uri $uri/ /index.html;
        }

        location /api {
            proxy_pass http://hpal-backend:8000;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        }

        # Environment variables passed at runtime
        location /config {
            default_type application/json;
            return 200 '{"apiUrl":"$API_BASE_URL"}';
        }
    }
}
```

#### Build & Push to ACR

```bash
# Build
docker build -t hpal-frontend:latest .

# Tag for ACR
docker tag hpal-frontend:latest myregistry.azurecr.io/hpal-frontend:latest

# Login to ACR
az acr login --name myregistry

# Push
docker push myregistry.azurecr.io/hpal-frontend:latest
```

#### Deploy to Container Apps

```bash
az containerapp create \
  --name hpal-frontend \
  --resource-group rg-hpal \
  --image myregistry.azurecr.io/hpal-frontend:latest \
  --environment hpal-env \
  --target-port 80 \
  --ingress external \
  --query properties.configuration.ingress.fqdn
```

---

### Option 3: Azure App Service

#### Using Local Git

```bash
# Create App Service Plan
az appservice plan create \
  --name hpal-plan \
  --resource-group rg-hpal \
  --sku B1 \
  --is-linux

# Create Web App
az webapp create \
  --name hpal-frontend \
  --resource-group rg-hpal \
  --plan hpal-plan \
  --runtime "node|18-lts"

# Configure deployment
az webapp deployment source config-local-git \
  --name hpal-frontend \
  --resource-group rg-hpal

# Deploy
git remote add azure https://USERNAME@hpal-frontend.scm.azurewebsites.net/hpal-frontend.git
git push azure main
```

#### Application Settings

```bash
az webapp config appsettings set \
  --name hpal-frontend \
  --resource-group rg-hpal \
  --settings VITE_API_BASE_URL="https://hpal-backend.azurewebsites.net/api"
```

---

### Option 4: Netlify (For Public Preview)

#### Setup

1. **Connect Repository**
   - Go to [Netlify](https://netlify.com)
   - Click "New site from Git"
   - Select repository

2. **Configure Build Settings**
   - Project: `hpal-frontend`
   - Build command: `npm run build`
   - Publish directory: `dist`
   - Node version: `18`

3. **Environment Variables**
   - Go to Site Settings → Build & Deploy → Environment
   - Add `VITE_API_BASE_URL=https://hpal-backend.example.com/api`

4. **Deploy**
   - Push to main branch
   - Netlify automatically builds and deploys

---

### Option 5: Vercel (Alternative Hosting)

#### Setup

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel --prod
```

Follow prompts to connect GitHub account and configure project.

---

## Environment Configuration

### Build-Time Variables

```bash
# .env
VITE_API_BASE_URL=http://localhost:8000/api
```

### Runtime Environment (Docker / Container Apps)

Use environment variables passed at container startup:

```bash
docker run \
  -e VITE_API_BASE_URL=https://hpal-backend.example.com/api \
  -p 80:80 \
  hpal-frontend:latest
```

### Azure Static Web Apps

Via Application Settings in Azure Portal:

```
VITE_API_BASE_URL = https://hpal-backend.azurewebsites.net/api
```

---

## CORS Configuration

Frontend must be added to CORS allowlist on HPAL backend:

**Node.js Express Backend:**

```javascript
import cors from "cors";

app.use(cors({
  origin: [
    "http://localhost:5173",       // Local dev
    "https://hpal-frontend.azurestaticapps.net",  // Azure Static Web Apps
    "https://hpal-frontend.netlify.app",          // Netlify
  ],
  credentials: true,
}));
```

**Python FastAPI Backend:**

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "https://hpal-frontend.azurestaticapps.net",
        "https://hpal-frontend.netlify.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## Health Checks

### Azure Static Web Apps

Frontend automatically health checked via `/index.html`.

### Container Apps / App Service

Add health check endpoint in nginx config:

```nginx
location /health {
    return 200 "OK";
    add_header Content-Type text/plain;
}
```

Check endpoint:

```bash
curl https://hpal-frontend.example.com/health
```

---

## Monitoring & Logging

### Azure Application Insights

1. **Create Application Insights resource**

```bash
az monitor app-insights component create \
  --app hpal-frontend \
  --location eastus \
  --resource-group rg-hpal
```

2. **Add to frontend** (optional for error tracking)

```bash
npm install @microsoft/applicationinsights-web
```

3. **Initialize in App.tsx**

```typescript
import { ApplicationInsights } from "@microsoft/applicationinsights-web";

const ai = new ApplicationInsights({
  config: {
    instrumentationKey: process.env.REACT_APP_APPINSIGHTS_KEY,
  },
});
ai.loadAppInsights();
```

### Logs

Monitor via:

- **Azure Portal** → Application Insights → Logs
- **CLI**: `az monitor app-insights metrics show --app hpal-frontend`

---

## SSL/TLS

### Azure Static Web Apps

Managed SSL (*.azurestaticapps.net) included automatically.

### Custom Domain

```bash
az staticwebapp custom-domain create \
  --name hpal-frontend \
  --custom-domain mycompany.com \
  --resource-group rg-hpal
```

### Container Apps / App Service

Use Azure Application Gateway or HTTPS-enabled custom domain.

---

## Regional Deployments

### Multi-Region (Azure Static Web Apps)

Static Web Apps automatically deploys to CDN across all regions.

### Multi-Region (Container Apps)

Deploy multiple container app revisions:

```bash
az containerapp revision list \
  --name hpal-frontend \
  --resource-group rg-hpal
```

---

## Rollback

### GitHub Actions

Revert commit and push Git:

```bash
git revert HEAD
git push origin main
```

GitHub Actions rebuilds and redeploys.

### Manual (Static Web Apps)

Azure Portal → Staging → Swap (if using staging slot).

---

## Cost Optimization

- **Azure Static Web Apps**: Free tier for development, $0.20/GB for production
- **Container Apps**: Pay-per-second pricing (~$0.0000694/sec per vCPU)
- **App Service**: B1 (~$50/month), auto-scale to higher SKUs as needed

Recommendation: Start with **Azure Static Web Apps** for production, or **Container Apps** if integrating with microservices.

---

## Troubleshooting

### 404 on Custom Routes

Ensure `try_files` in nginx or `_redirects` in Static Web Apps:

**Static Web Apps (_redirects)**

```
/*  /index.html  200
```

### API Base URL Not Found

Check `.env` or environment variables:

```bash
# Local
VITE_API_BASE_URL=http://localhost:8000/api

# Production
VITE_API_BASE_URL=https://hpal-backend.azurewebsites.net/api
```

### CORS Errors

Verify HPAL backend CORS allowlist includes frontend URL.

### Build Fails

Check Node version (require 18+):

```bash
node --version  # Should be v18.x.x or higher
```

---

## Support

For issues, check:

1. HPAL backend logs
2. Frontend browser console (F12)
3. Azure Portal → Application Insights

