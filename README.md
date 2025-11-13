# 🏢 AdventureWorks Data Platform

> A modern, scalable data platform implementing Bronze-Silver-Gold architecture with automated ETL pipelines, real-time analytics, and business intelligence dashboards.

## 🎯 Project Overview

This project demonstrates a complete end-to-end data engineering solution for AdventureWorks, featuring:

- **Multi-source data ingestion** from APIs, databases, and flat files
- **Medallion architecture** (Bronze-Silver-Gold) for data quality and governance
- **Automated ETL pipelines** using Apache Airflow
- **Real-time analytics** with PostgreSQL and business intelligence dashboards
- **Containerized deployment** for scalability and portability

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  Landing Layer  │    │  Bronze Layer   │    │  Silver Layer   │
│                 │    │     (MinIO)     │    │  (PostgreSQL)   │    │  (PostgreSQL)   │
│ • APIs          │───▶│ • Raw Files     │───▶│ • Schema        │───▶│ • Clean Data    │
│ • Databases     │    │ • Parquet       │    │ • Validation    │    │ • Deduplication │
│ • CSV Files     │    │ • Partitioned   │    │ • Type Safety   │    │ • SCD Type 2    │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                                              │
                                                                              ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Metabase      │    │   Gold Layer    │    │    Airflow      │    │   Monitoring    │
│                 │    │  (PostgreSQL)   │    │   Orchestrator  │    │                 │
│ • Dashboards    │◀───│ • Business KPIs │    │ • 9 DAGs        │    │ • Data Quality  │
│ • Reports       │    │ • Aggregations  │    │ • Dependencies  │    │ • Pipeline      │
│ • Visualizations│    │ • Analytics     │    │ • Scheduling    │    │ • Alerts        │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM recommended
- Ports 8080, 3000, 5050, 5432, 9000, 9001 available

### 1. Clone & Start
```bash
git clone https://github.com/yourusername/AdventureWorks-Data-Platform.git
cd AdventureWorks-Data-Platform
docker-compose up -d
```

### 2. Access Services
- **Airflow**: http://localhost:8080 (admin/admin)
- **Metabase**: http://localhost:3000
- **pgAdmin**: http://localhost:5050 (admin@admin.com/admin)
- **MinIO**: http://localhost:9001 (minioadmin/minioadmin)

### 3. Run Data Pipeline
1. Enable all DAGs in Airflow
2. Trigger department pipelines (HR, Sales, Finance, Customer, Production)
3. Monitor data flow through Bronze → Silver → Gold layers
4. View dashboards in Metabase

## 📊 Data Pipeline Components

### Department Pipelines (5 DAGs)
| Department | Data Sources | Records | Status |
|------------|--------------|---------|--------|
| **HR** | Employee API, Person DB | 290+ | ✅ |
| **Sales** | Orders API, Customer DB | 31K+ | ✅ |
| **Finance** | Transaction Files | 5K+ | ✅ |
| **Customer** | CRM API, Demographics | 19K+ | ✅ |
| **Production** | Manufacturing DB | 500+ | ✅ |

### Layer Processing DAGs (4 DAGs)
| Layer | Purpose | Tables/Views | Status |
|-------|---------|--------------|--------|
| **Landing** | Raw data ingestion | Parquet files | ✅ |
| **Bronze** | Raw data validation | 15+ tables | ✅ |
| **Silver** | Clean, deduplicated data | 9 dimensions + 2 facts | ✅ |
| **Gold** | Business intelligence | 3 analytical views | ✅ |

## 🎯 Key Features

### ✅ Data Ingestion
- **Multi-source**: APIs, PostgreSQL, CSV files
- **Automated**: Scheduled and triggered pipelines
- **Scalable**: Containerized microservices architecture
- **Reliable**: Error handling and retry mechanisms

### ✅ Data Processing
- **Bronze Layer**: Schema enforcement, data validation
- **Silver Layer**: Data cleaning, deduplication, SCD Type 2
- **Gold Layer**: Business KPIs, aggregations, analytics

### ✅ Data Storage
- **Landing**: MinIO object storage (S3-compatible)
- **Warehouse**: PostgreSQL with optimized schemas
- **Partitioning**: Date-based partitioning for performance

### ✅ Orchestration
- **Apache Airflow**: 9 production DAGs
- **Dependency Management**: Proper task sequencing
- **Monitoring**: Built-in logging and alerting

### ✅ Analytics & BI
- **Metabase Dashboards**: Interactive visualizations
- **KPI Tracking**: Sales, customer, product metrics
- **Real-time**: Live data updates

## 📈 Business Intelligence Views

### Gold Layer Analytics

#### 🏆 Top Products Performance
```sql
SELECT product_name, total_revenue, total_quantity_sold, product_tier
FROM gold_top_products
ORDER BY total_revenue DESC;
```

#### 👥 Customer Insights
```sql
SELECT customer_segment, COUNT(*) as customers, SUM(total_spent) as revenue
FROM gold_customer_analysis
GROUP BY customer_segment;
```

#### 📊 Sales Trends
```sql
SELECT year_month, territory_name, total_revenue, total_orders
FROM gold_sales_trends
ORDER BY year_month DESC;
```

## 🛠️ Technical Stack

| Component | Technology | Purpose |
|-----------|------------|----------|
| **Orchestration** | Apache Airflow | Workflow management |
| **Storage** | PostgreSQL | Data warehouse |
| **Object Store** | MinIO | Raw file storage |
| **Visualization** | Metabase | Business intelligence |
| **Containerization** | Docker Compose | Service deployment |
| **Languages** | Python, SQL | Data processing |

## 📁 Project Structure

```
AdventureWorks-Data-Platform/
├── airflow/dags/              # Airflow DAGs
│   ├── hr_dag.py
│   ├── sales_dag.py
│   ├── bronze_layer_complete_dag.py
│   ├── silver_layer_complete_dag.py
│   └── gold_layer_simple_working_dag.py
├── scripts/                   # Data ingestion scripts
│   ├── hr_ingestion.py
│   ├── sales_ingestion.py
│   └── ...
├── utils/                     # Utility modules
│   ├── db_utils.py
│   ├── minio_utils.py
│   └── config.py
├── sql/                       # SQL scripts
│   ├── verification/
│   └── fixes/
├── notebooks/                 # Analysis & Visualization
├── docs/                      # Documentation
└── docker-compose.yml         # Service definitions
```

## 🔧 Configuration

### Environment Variables
```bash
# Database
POSTGRES_USER=postgres
POSTGRES_PASSWORD=1004
POSTGRES_DB=postgres

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Airflow
AIRFLOW_UID=50000
```

## 📊 Data Quality & Monitoring

### Automated Checks
- ✅ Schema validation in Bronze layer
- ✅ Data type enforcement
- ✅ Duplicate detection in Silver layer
- ✅ Referential integrity checks
- ✅ Business rule validation

## 🎯 Business KPIs Delivered

### Sales Analytics
- Revenue trends by month/quarter
- Top-performing products and categories
- Regional sales performance
- Customer segmentation analysis

### Operational Metrics
- Employee performance tracking
- Production efficiency metrics
- Customer acquisition costs
- Inventory turnover rates

## 🚀 Deployment

### Local Development
```bash
docker-compose up -d
```

### Stopping Services
```bash
docker-compose down
```

## 📝 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- AdventureWorks sample database by Microsoft
- Apache Airflow community
- Docker and containerization ecosystem

---

**Built with ❤️ for modern data engineering**

*This project demonstrates enterprise-level data platform capabilities including automated ETL, real-time analytics, and scalable architecture patterns.*
