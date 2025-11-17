# 🚀 Deployment Guide

## 📋 Quick Deployment Checklist

### **✅ Prerequisites**
- [ ] Python 3.8+ installed
- [ ] Snowflake account created
- [ ] Git installed
- [ ] Terminal/Command line access

### **⚙️ Environment Setup**
```bash
# 1. Clone repository
git clone https://github.com/dangtuan21/datalake-demo.git
cd datalake-demo

# 2. Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

### **🔧 Configuration**
```bash
# 1. Copy environment template
cp config/.env.example config/.env

# 2. Edit with your credentials
nano config/.env  # Or use your preferred editor
```

**Required .env variables:**
```env
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_DATALAKE
SNOWFLAKE_SCHEMA=RAW_DATA
```

### **🗄️ Database Setup**
```bash
# Deploy Snowflake schema
python etl/test_snowflake_connection.py
```

### **📊 Data Processing**
```bash
# 1. Convert Excel to CSV
python etl/data_processor.py

# 2. Load initial batches
python etl/incremental_etl_pipeline.py --batch 1
python etl/incremental_etl_pipeline.py --batch 2
```

### **🚀 Start Services**

**Terminal 1 - API Server:**
```bash
python api/main.py
# Access at: http://localhost:8000
```

**Terminal 2 - Dashboard:**
```bash
streamlit run dashboard/app.py
# Access at: http://localhost:8501
```

## 🔍 Verification Steps

### **1. Test API**
```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/summary
```

### **2. Check Dashboard**
- Open http://localhost:8501
- Navigate through all 5 pages
- Verify data displays correctly

### **3. Test ETL**
```bash
python etl/incremental_etl_pipeline.py --status
```

## 🐛 Troubleshooting

### **Common Issues:**

**❌ Snowflake Connection Error**
```bash
# Check credentials in .env file
# Verify account URL format
# Ensure warehouse is running
```

**❌ Module Import Error**
```bash
# Activate virtual environment
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

**❌ Port Already in Use**
```bash
# Kill existing processes
lsof -ti:8000 | xargs kill -9  # API
lsof -ti:8501 | xargs kill -9  # Dashboard
```

**❌ Data File Not Found**
```bash
# Ensure Excel file exists
ls data/raw/online-retail.xlsx

# Re-run data processor
python etl/data_processor.py
```

## 📈 Production Deployment

### **Cloud Deployment Options:**

**🔌 API (FastAPI)**
- **Heroku**: `git push heroku main`
- **AWS Lambda**: Use Mangum adapter
- **Google Cloud Run**: Deploy with Docker
- **Azure Container Instances**: Container deployment

**📊 Dashboard (Streamlit)**
- **Streamlit Cloud**: Connect GitHub repo
- **Heroku**: Use streamlit buildpack
- **AWS EC2**: Direct deployment
- **Google Cloud Run**: Container deployment

**🗄️ Database (Snowflake)**
- Already cloud-hosted ✅
- Configure production credentials
- Set up automated backups
- Monitor usage and costs

### **Environment Variables for Production:**
```env
# Production Snowflake
SNOWFLAKE_ACCOUNT=prod-account.snowflakecomputing.com
SNOWFLAKE_USER=prod-user
SNOWFLAKE_PASSWORD=secure-password
SNOWFLAKE_WAREHOUSE=PROD_WH

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO

# Dashboard Configuration
DASHBOARD_PORT=8501
CACHE_TTL=300
```

## 🔐 Security Checklist

### **🛡️ Production Security:**
- [ ] Use environment variables for all secrets
- [ ] Enable Snowflake MFA
- [ ] Rotate passwords regularly
- [ ] Use HTTPS in production
- [ ] Implement API rate limiting
- [ ] Set up monitoring and alerting
- [ ] Regular security audits

### **📊 Monitoring:**
- [ ] Set up application logging
- [ ] Monitor API response times
- [ ] Track ETL job success/failure
- [ ] Monitor Snowflake usage and costs
- [ ] Set up health check endpoints

## 📞 Support

**Need help?**
- 📖 Check the main [README.md](README.md)
- 🐛 Create an issue on GitHub
- 📧 Contact: [Your contact info]

---

**Happy deploying! 🚀**
