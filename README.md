# Crime × Weather × Demographics ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline that integrates multiple data sources to create a unified dataset for analyzing crime patterns in relation to weather conditions and demographic characteristics. This project processes large-scale crime data, weather records, and U.S. Census Bureau American Community Survey (ACS) demographics, then performs spatial joins to map crimes to census tracts using Apache Sedona for geospatial operations.

## 📋 Project Overview

This pipeline processes **8.4+ million crime records** and integrates them with:
- Daily weather observations (precipitation, temperature, snowfall)
- Census tract boundaries (TIGER/Line shapefiles)
- ACS demographic data (median income, population)

The result is a denormalized, wide table ready for advanced analytics, enabling analysis of:
- Temporal crime patterns (hourly, daily, seasonal trends)
- Weather-crime correlations
- Demographic factors influencing crime rates
- Spatial distribution of crime hotspots
## Note: 
- We used google drive to store the dataset and parquet file. Ideally this has to be a HDFS storage. We have attempted to use HDFS . We could not implement the whole solution using HDFS due to resource constraints. Refer HDFS setup instructions md file for details.
## 🎯 Key Objectives

1. **Data Integration**: Combine crime incidents, weather data, and demographic information into a single denormalized dataset
2. **Spatial Analysis**: Map crime locations to census tracts using geospatial operations
3. **Scalability**: Handle large datasets efficiently using PySpark distributed processing
4. **Data Quality**: Ensure proper data cleaning, validation, and integration success tracking

## 📊 Data Sources

- **Crime Data**: `CrimeDataFull.csv` - Contains crime incident records with location, type, date, and other attributes (8.4M+ records)
- **Weather Data**: `weather.csv` - Daily weather observations including precipitation, temperature, and snowfall (8,766 distinct days)
- **ACS Demographics**:
  - Median Income: `Median_Income/` directory (multiple CSV files)
  - Population: `Population_Data/` directory (multiple CSV files)
- **Census Tract Boundaries**: `Census_Tracts/` - TIGER/Line shapefiles containing census tract geometries (3,265 tracts)
- **Output Directory**: `output/` - Stores processed Parquet files and analysis results

## 🛠️ Technology Stack

- **PySpark 3.5.1**: Distributed data processing framework
- **Apache Sedona 1.8.0**: Geospatial data processing library for Spark
- **OpenJDK 11**: Java runtime for Spark
- **Pandas**: Data manipulation and analysis
- **GeoPandas/Shapely/Fiona**: Geospatial data handling and shapefile reading
- **Matplotlib/Seaborn**: Statistical plotting and visualization
- **Folium**: Interactive map generation

## 🚀 Getting Started

### Prerequisites

- Google Colab environment (or local Spark cluster)
- Google Drive with data files mounted (We used google drive to store the dataset and parquet file. Ideally this has to be a HDFS storage. We have attempted to use HDFS . We could not implement the whole solution using HDFS due to resource constraints. Refer HDFS setup instructions md file for details)
- Python 3.x

### Installation

The notebook automatically installs all required dependencies. Key packages include:

```bash
pyspark==3.5.1
apache-sedona==1.8.0
pandas
geopandas
matplotlib
seaborn
folium
```

### Data Setup

Ensure your data files are organized in Google Drive as follows:

```
MyDrive/
├── CrimeDataFull.csv
├── weather.csv
├── Median_Income/
│   └── *.csv (ACS income data files)
├── Population_Data/
│   └── *.csv (ACS population data files)
└── Census_Tracts/
    └── *.shp, *.dbf, *.shx (TIGER/Line shapefiles)
```

## 📝 Pipeline Architecture

### Step 1: Environment Setup
- Installs OpenJDK 11, PySpark, Apache Sedona, and visualization libraries
- Configures Java environment for Spark

### Step 2: Data Path Configuration
- Mounts Google Drive
- Defines input/output paths for all data sources

### Step 3: Spark & Sedona Initialization
- Creates Spark session with optimized configurations
- Initializes SedonaContext for geospatial operations
- Configures memory, serialization, and shuffle partitions

### Step 4: Load Raw Data
- Reads crime CSV with explicit schema (8.4M+ records)
- Loads weather CSV with automatic date column detection
- Selects only essential columns for memory optimization

### Step 5: Crime Data Transformation
- Parses timestamps and extracts temporal features (year, month, hour, weekday)
- Normalizes boolean columns (Arrest, Domestic)
- Removes duplicate records
- Standardizes column names

### Step 6: Weather Data Processing
- Handles multiple date formats with fallback parsing
- Casts numeric weather metrics (PRCP, SNOW, TMAX, TMIN)
- Aggregates daily weather observations
- Performs broadcast join with crime data (optimized for small weather table)

### Step 7: ACS Demographics Loading
- Reads multiple ACS CSV files from directories
- Extracts year from filenames
- Filters header rows and selects estimate columns
- Unions data from multiple years
- Joins income and population data

### Step 8: Spatial Join (Critical Step)
- Loads census tract shapefiles using Sedona
- Standardizes GEOID column names
- Creates point geometries from crime coordinates
- Performs spatial containment join using `ST_Contains`
- Uses broadcast join optimization
- **Checkpoints intermediate results** to prevent memory overflow

### Step 9: Demographics Integration
- Extracts 11-digit GEOID from ACS format
- Aggregates demographics by tract
- Joins with spatially-joined crime data

### Step 10: Save Final Dataset
- Writes integrated dataset to Parquet format
- Partitions by year for optimized queries
- Enables fast access for subsequent analysis

### Step 11: Data Quality Validation
- Verifies total record count
- Calculates integration success rates:
  - Spatial join: 100% success
  - Demographic join: 100% success
  - Weather join: 100% success
- Validates schema completeness

### Steps 12-21: Exploratory Data Analysis
- Top crime types analysis
- Yearly and hourly crime trends
- Weather-crime correlations
- Crime rate analysis by census tract
- Temperature and precipitation impact studies
- Interactive and static map visualizations

## 📈 Key Results

### Integration Metrics
- **Total Integrated Rows**: 8,344,980
- **Spatial Join Success**: 100%
- **Demographic Join Success**: 100%
- **Weather Join Success**: 100%

### Performance Metrics
- **Spatial Join & Checkpoint**: ~5.93 minutes
- **Scalability**: Successfully processes 8.4M+ records

### Top Crime Types
1. THEFT (1,770,279 incidents)
2. BATTERY (1,530,899 incidents)
3. CRIMINAL DAMAGE (954,951 incidents)
4. NARCOTICS (751,303 incidents)
5. ASSAULT (563,292 incidents)

## 🔍 Key Features

### Data Processing Optimizations
- **Selective Column Loading**: Only loads essential columns to reduce memory footprint
- **Broadcast Joins**: Optimizes joins with small tables (weather, tracts)
- **Checkpointing**: Writes intermediate results to prevent DAG complexity issues
- **Partitioning**: Year-based partitioning for efficient time-range queries
- **Caching**: Caches frequently accessed small datasets

### Spatial Operations
- **Point-in-Polygon Queries**: Maps crime locations to census tracts using `ST_Contains`
- **GEOID Standardization**: Handles various shapefile formats and ACS data formats
- **Geometry Creation**: Converts lat/lon coordinates to Sedona point geometries

### Data Quality
- **Schema Enforcement**: Explicit schemas prevent parsing errors
- **Deduplication**: Removes duplicate crime records
- **Null Handling**: Preserves records with missing data using left joins
- **Validation Reports**: Comprehensive quality checks at each stage

## 📊 Visualizations

The pipeline generates multiple visualizations:

1. **Time Series**: Daily crime trends over 20+ years
2. **Interactive Maps**: Folium maps showing crime locations and high-crime tracts
3. **Static Maps**: Publication-ready maps of census tract crime rates
4. **Bar Charts**: Crime frequency by type, temperature, and precipitation
5. **Correlation Plots**: Temperature-crime relationships over time

## 🎓 Usage

1. Open the notebook in Google Colab
2. Mount your Google Drive with data files
3. Run cells sequentially from top to bottom
4. Review validation reports to ensure data quality
5. Explore visualizations and analysis results

## 📁 Output Files

The pipeline generates:
- `crime_weather_parquet/`: Crime + Weather joined data
- `crime_weather_demo_parquet/`: Crime + Weather + Demographics (pre-spatial)
- `crime_weather_demo_spatial/`: Final integrated dataset (partitioned by year)
- `intermediate_crime_spatial/`: Checkpointed spatial join results
- `daily_incidents_all_time.csv`: Daily crime counts for external analysis

## 🔧 Configuration

Key Spark configurations:
- **Driver Memory**: 8GB
- **Shuffle Partitions**: 200
- **Serializer**: KryoSerializer (optimized for spatial data)
- **Legacy Time Parser**: Enabled for older date formats

## 📝 Notes

- Ensure shapefile GEOID matches ACS GEO_ID extraction format
- For larger datasets, increase `spark.sql.shuffle.partitions` and use distributed storage (HDFS/S3/GS)
- Additional ACS variables (education, unemployment) can be added by reading corresponding ACS tables
- Consider caching intermediate DataFrames for iterative analysis

## 🤝 Contributing

This is a course project for Data Engineering. For questions or improvements, please open an issue or submit a pull request.

## 📄 License

This project is part of a Data Engineering course assignment.

---

**Author**: Vimalraj Kanagaraj  
**Project**: DES - Crime, Weather, and Demographics Data Ingestion  
**Date**: 2024

