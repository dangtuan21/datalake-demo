# 🏪 Retail Data Lake Demo

A production-ready data lake implementation using **Snowflake**, **Python**, and modern analytics tools for processing 541K+ retail transactions.

## 🎯 Overview

This project demonstrates building a scalable, enterprise-grade data lake for retail analytics, featuring:

- **☁️ Snowflake** - Cloud data warehouse with optimized schemas
- **🔄 Incremental ETL** - Manual batch processing (configurable batch size) with deduplication
- **🔌 REST API** - FastAPI endpoints for data access
- **📊 Interactive Dashboard** - Streamlit visualization interface
- **📈 Real-time Analytics** - Live metrics and progress tracking

## 🏗️ Architecture

```
Excel Data (541K rows) → Python ETL → Snowflake → FastAPI → Streamlit Dashboard
                         (Batches)    (4 Schemas)   (15+ APIs)   (5 Pages)
```

### **Data Flow**
1. **Raw Data**: Excel file with retail transactions
2. **ETL Processing**: Incremental loading in configurable batches (default: 200 records)
3. **Data Warehouse**: Snowflake with RAW_DATA, PROCESSED_DATA, ANALYTICS, METADATA schemas
4. **API Layer**: RESTful endpoints for data access
5. **Visualization**: Interactive dashboard with real-time charts

## 📊 Current Status

| Metric | Value | Progress |
|--------|-------|----------|
| **Records Processed** | 2,000 / 541,909 | 0.4% |
| **Batches Completed** | 2 | Batch 1 & 2 ✅ |
| **Products Discovered** | 85 | Growing with each batch |
| **Customers** | 798 | Unique customer profiles |
| **Countries** | 25 | Geographic coverage |
| **Total Sales** | $38,560.55 | Revenue processed |

## 🚀 Quick Start

### **1. Prerequisites**
- Python 3.8+
- Snowflake account
- Git

### **2. Installation**
```bash
# Clone the repository
git clone https://github.com/dangtuan21/datalake-demo.git
cd datalake-demo

# Install dependencies
pip install -r requirements.txt
```

### **3. Configuration**
```bash
# Copy environment template
cp config/.env.example config/.env

# Edit with your Snowflake credentials
nano config/.env
```

### **4. Setup Database**
```bash
# Deploy Snowflake schema
python etl/test_snowflake_connection.py
```

### **5. Load Initial Data**
```bash
# Process Excel to CSV
python etl/data_processor.py

# Load first batch
python etl/incremental_etl_pipeline.py --batch 1
```

## 🎮 Usage

### **📊 Check Status**
```bash
python etl/incremental_etl_pipeline.py --status
```

### **🔄 Load Next Batch**
```bash
python etl/incremental_etl_pipeline.py --next
```

### **🔌 Start API Server**
```bash
python api/main.py
# API available at: http://localhost:8000
```

### **📈 Launch Dashboard**
```bash
streamlit run dashboard/app.py
# Dashboard available at: http://localhost:8501
```

## 📁 Project Structure

```
datalake-demo/
├── 📊 data/
│   ├── raw/                    # Original Excel files
│   ├── processed/              # CSV and profiles
│   └── schemas/                # SQL schema definitions
├── 🔄 etl/
│   ├── data_processor.py       # Excel to CSV converter
│   ├── incremental_etl_pipeline.py  # Main ETL pipeline
│   └── test_snowflake_connection.py # Schema deployment
├── 🔌 api/
│   └── main.py                 # FastAPI REST endpoints
├── 📊 dashboard/
│   └── app.py                  # Streamlit dashboard
├── ⚙️ config/
│   ├── snowflake_config.py     # Database connection
│   └── .env.example            # Environment template
├── 📋 requirements.txt         # Python dependencies
└── 📖 README.md               # This file
```

## 🔌 API Endpoints

### **Core Data**
- `GET /api/summary` - Data lake overview
- `GET /api/products` - Product analytics
- `GET /api/customers` - Customer insights
- `GET /api/countries` - Geographic analysis

### **Sales Analytics**
- `GET /api/sales/metrics` - Overall sales metrics
- `GET /api/sales/daily` - Daily sales trends
- `GET /api/sales/monthly` - Monthly revenue

### **ETL Monitoring**
- `GET /api/batches/status` - Batch processing history
- `GET /api/batches/latest` - Latest batch information
- `GET /health` - System health check

## 📊 Dashboard Pages

1. **📈 Overview** - Key metrics, trends, top products
2. **📦 Products** - Product performance analysis
3. **👥 Customers** - Customer segmentation and behavior
4. **🌍 Countries** - Geographic sales distribution
5. **🔄 ETL Status** - Pipeline monitoring and controls

## 🛠️ Technical Features

### **🔄 Incremental ETL**
- **Batch Processing**: Configurable batch size (default: 200 records) for manageable loads
- **Deduplication**: Prevents duplicate data insertion
- **UPSERT Logic**: Smart updates for dimension tables
- **Error Recovery**: Graceful handling of failures
- **Progress Tracking**: Visual progress monitoring

### **🏗️ Data Architecture**
- **RAW_DATA**: Staging tables for incoming data
- **PROCESSED_DATA**: Clean dimension and fact tables
- **ANALYTICS**: Pre-built analytical views
- **METADATA**: Pipeline execution logs and data catalog

### **⚡ Performance**
- **Optimized Queries**: Efficient SQL with proper indexing
- **Caching**: API response caching for faster dashboard loads
- **Scalable Design**: Ready for millions of records

## ⚙️ Configuration

### **Batch Size Configuration**
To change the batch size, modify the `BATCH_SIZE` constant in:
- `etl/incremental_etl_pipeline.py` (line 25)
- `dashboard/app.py` (line 20)

```python
# Configuration constants
BATCH_SIZE = 200  # Change this value as needed
```

**Recommended batch sizes:**
- **Small datasets**: 100-500 records
- **Medium datasets**: 500-2000 records  
- **Large datasets**: 1000-5000 records

## 🔧 Advanced Usage

### **Load Specific Batch**
```bash
python etl/incremental_etl_pipeline.py --batch 5
```

### **Bulk Load Multiple Batches**
```bash
# Load batches 3-10
for i in {3..10}; do
    python etl/incremental_etl_pipeline.py --batch $i
done
```

### **API Testing**
```bash
# Test endpoints
curl http://localhost:8000/api/summary
curl http://localhost:8000/api/products?limit=5
curl http://localhost:8000/health
```

## 📈 Scaling to Production

### **Complete Data Loading**
The system is designed to handle the full 541K+ records:
- **Estimated Time**: ~45 hours for all 2,710 batches
- **Storage**: ~500MB in Snowflake
- **Performance**: Optimized for large-scale processing

### **Production Deployment**
- Deploy API to cloud (AWS/GCP/Azure)
- Use managed Streamlit hosting
- Implement automated scheduling
- Add monitoring and alerting

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Snowflake** for the cloud data platform
- **FastAPI** for the modern web framework
- **Streamlit** for the interactive dashboard
- **Plotly** for beautiful visualizations

## 📞 Support

For questions or issues:
- Create an issue in this repository
- Check the documentation in `/docs`
- Review the example configurations

---

**Built with ❤️ using modern data engineering best practices**
