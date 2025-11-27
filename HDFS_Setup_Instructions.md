# Crime × Weather × Demographics Pipeline – Infrastructure & Setup Guide

This document explains how to reproduce our infrastructure:

- Multi-VM Hadoop HDFS cluster in VirtualBox
- Spark + Sedona on the Pop!_OS host
- Jupyter Lab on the host, connecting to HDFS
- Migration of the original Colab notebook to this environment

---

## 1. VirtualBox & VM Setup

### 1.1 Install VirtualBox on Pop!_OS

1. Install VirtualBox (and Extension Pack if needed) from the official repo.
2. Fix **KVM vs VirtualBox** conflict (Pop!_OS used KVM by default):
   ```bash
   sudo modprobe -r kvm kvm_intel   # or kvm_amd
   ```
   Add to `/etc/modprobe.d/blacklist.conf`:
   ```text
   blacklist kvm
   blacklist kvm_intel   # or kvm_amd
   ```
   Reboot so VirtualBox can use hardware virtualization.

### 1.2 Create VMs

Create at least 3 VMs:

- `namenode`
- `datanode1`
- `datanode2`

Example resources:

- Namenode: 2–4 vCPUs, 4–8 GB RAM
- Datanodes: 1–2 vCPUs, 2–4 GB RAM

Install basic packages inside each VM:

```bash
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk-headless openssh-server
```

### 1.3 Networking (Static IPs + Host-only)

We use a **Host-only Adapter** so:

- All VMs see each other.
- Host (Pop!_OS) can access the cluster.

Steps:

1. In VirtualBox:
   - For each VM: Network → Adapter 1 → Host-only Adapter → `vboxnet0`.
2. Configure **static IPs** inside each VM (example):
   - `namenode`: `192.168.56.101`
   - `datanode1`: `192.168.56.102`
   - `datanode2`: `192.168.56.103`
3. Add to [/etc/hosts](cci:7://file:///etc/hosts:0:0-0:0) **on all VMs and the host**:
   ```text
   192.168.56.101 namenode
   192.168.56.102 datanode1
   192.168.56.103 datanode2
   ```
4. Test from host:
   ```bash
   ping namenode
   ping datanode1
   ```

---

## 2. Hadoop / HDFS Cluster

### 2.1 Install Hadoop & Configure Environment

On each VM:

```bash
# download + unpack Hadoop to /opt/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

Add those exports to `~/.bashrc` and `source ~/.bashrc`.

### 2.2 Hadoop Config Files

`$HADOOP_HOME/etc/hadoop/core-site.xml` (on all nodes):

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:9000</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///var/hadoop/dfs/name</value>
  </property>

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///var/hadoop/dfs/data</value>
  </property>
</configuration>
```

Workers list on namenode: `$HADOOP_HOME/etc/hadoop/workers`:

```text
datanode1
datanode2
```

Create local dirs:

```bash
sudo mkdir -p /var/hadoop/dfs/name /var/hadoop/dfs/data
sudo chown -R $USER /var/hadoop/dfs
```

### 2.3 Format & Start HDFS

On `namenode`:

```bash
hdfs namenode -format
start-dfs.sh
```

Verify:

```bash
hdfs dfsadmin -report
hdfs dfs -ls /
```

### 2.4 HDFS Directory Layout & Data Upload

Create raw and user dirs:

```bash
hdfs dfs -mkdir /raw
hdfs dfs -mkdir /raw/crime /raw/weather /raw/acs_income /raw/acs_pop /raw/census_tracts

hdfs dfs -mkdir -p /user/cosmo
hdfs dfs -chown cosmo /user/cosmo
```

Upload data (from a VM with `hdfs`):

```bash
hdfs dfs -put CrimeDataFull.csv                /raw/crime/
hdfs dfs -put weather.csv                      /raw/weather/
hdfs dfs -put Median_Income/*                  /raw/acs_income/
hdfs dfs -put Population_Data/*                /raw/acs_pop/
hdfs dfs -put Census_Tracts/*                  /raw/census_tracts/
```

---

## 3. Spark + Sedona on Pop!_OS Host

### 3.1 Python Environment & Spark

On host:

```bash
python3 -m venv ~/des-env
source ~/des-env/bin/activate

pip install pyspark==3.5.1 apache-sedona==1.6.1 \
            pandas findspark matplotlib folium \
            geopandas shapely fiona pyproj rtree
```

Install Spark to `~/spark` and set:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=~/spark
export PATH=$PATH:$SPARK_HOME/bin
```

### 3.2 SparkSession with HDFS + Sedona

In the notebook:

```python
import os
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

packages = "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1"

spark = (
    SparkSession.builder
    .appName("CrimeDataProject")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer.max", "512m")
    .config("spark.jars.packages", packages)
    .getOrCreate()
)

sedona = SedonaContext.create(spark)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
```

> Note: We **do not** add `sedona-viz` here because the matching artifact for Spark 3.5/Scala 2.12 isn’t available in Maven Central, and trying to include it caused `JAVA_GATEWAY_EXITED`.

---

## 4. Jupyter Lab & Spark Web UI

### 4.1 Jupyter Lab

On Pop!_OS host:

```bash
source ~/des-env/bin/activate
jupyter lab --no-browser --port 8888
```

From Mac:

```bash
ssh -L 8888:localhost:8888 cosmo@<pop-os-ip>
# optionally also:
# ssh -L 8888:localhost:8888 -L 4040:localhost:4040 cosmo@<pop-os-ip>
```

Open `http://localhost:8888` in your browser.

### 4.2 Spark Web UI

After Spark starts:

```python
spark.sparkContext.uiWebUrl
```

Typically `http://pop-os:4040`. With SSH tunneling, open `http://localhost:4040` to see:

- Jobs, Stages, Executors, SQL, etc.
- Useful for monitoring heavy stages (e.g. spatial join).

---

## 5. Notebook Migration: Colab → HDFS

The original notebook was Colab + Google Drive based. We adapted it as follows.

### 5.1 Paths: Drive → HDFS

Replaced Drive paths with HDFS URIs:

```python
HDFS_BASE = "hdfs://namenode:9000"

CRIME_PATH          = f"{HDFS_BASE}/raw/crime/CrimeDataFull.csv"
WEATHER_PATH        = f"{HDFS_BASE}/raw/weather/weather.csv"
ACS_INCOME_DIR      = f"{HDFS_BASE}/raw/acs_income"
ACS_POP_DIR         = f"{HDFS_BASE}/raw/acs_pop"
TRACT_SHAPEFILE_DIR = f"{HDFS_BASE}/raw/census_tracts"

OUTPUT_PARQUET      = f"{HDFS_BASE}/user/cosmo/output/crime_weather_parquet"
OUTPUT_DEMO_PARQUET = f"{HDFS_BASE}/user/cosmo/output/crime_weather_demo_parquet"
OUTPUT_DEMO_SPATIAL = f"{HDFS_BASE}/user/cosmo/output/crime_weather_demo_spatial"
```

Removed:

- Colab `drive.mount`.
- `/content/drive/MyDrive/...` paths.

### 5.2 Crime + Weather ETL (unchanged logic)

Kept the same logic as the Colab version:

- **Crime**:
  - Explicit schema.
  - Select only needed columns: ID, Date, Primary Type, Arrest, Domestic, Latitude, Longitude.
  - Parse `crime_ts`, `crime_date`, `year`, `month`, `hour`, `weekday`.
  - Normalize `Arrest` / `Domestic` to booleans.

- **Weather**:
  - Heuristic date column detection.
  - Multi-format date parsing with fallbacks.
  - Cast PRCP/SNOW/TMAX/TMIN to double.
  - Aggregate daily averages.
  - Join to crime on `crime_date`.

Outputs:

- `final`: crime × daily weather.
- Written to `OUTPUT_PARQUET` in HDFS, partitioned by `year, month`.

---

## 6. ACS Demographics Integration

### 6.1 Reading ACS from HDFS

Function to read all `*Data.csv` under an HDFS folder:

```python
from pyspark.sql.functions import input_file_name, regexp_extract, col

def read_acs_folder(folder_path, estimate_col, label):
    pattern = f"{folder_path}/*Data.csv"
    df = spark.read.option("header", True).csv(pattern)

    df = df.filter(df.GEO_ID != "Geography")

    if estimate_col not in df.columns or "GEO_ID" not in df.columns or "NAME" not in df.columns:
        print(f"Expected columns missing in {pattern}")
        return None

    df = df.withColumn("source_file", input_file_name())
    df = df.withColumn(
        "source_file",
        regexp_extract(col("source_file"), r"([^/]+)\.csv$", 1)
    )
    df = df.withColumn("acs_year", regexp_extract(col("source_file"), r"(\d{4})", 1))

    df = (df
          .select("GEO_ID", "NAME", estimate_col, "source_file", "acs_year")
          .withColumnRenamed(estimate_col, label))
    return df
```

### 6.2 Fixing `acs_df` (0 rows → 18,518 rows)

Problem:

- Income CSVs are US-level (`GEO_ID = 0100000US`, one row per year).
- Population CSVs are tract-level (`GEO_ID = 1400000US17031010100`, many rows per year).
- Joining on `["GEO_ID", "acs_year"]` returns 0 rows.

Solution:

- Join by **`acs_year` only**:
  - One median income per year is applied to all tracts in that year.

Adapted code:

```python
income_df = read_acs_folder(ACS_INCOME_DIR, "B19013_001E", "Median_Income")
pop_df    = read_acs_folder(ACS_POP_DIR,   "B01003_001E", "Population_Data")

income_df.show(5, truncate=False)
pop_df.show(5, truncate=False)
print("Income rows:", income_df.count() if income_df else 0)
print("Population rows:", pop_df.count() if pop_df else 0)

if income_df and pop_df:
    income_year_df = (income_df
                      .select("Median_Income", "acs_year")
                      .dropDuplicates(["acs_year"]))

    acs_df = income_year_df.join(pop_df, "acs_year", "inner")
else:
    acs_df = None

acs_df.show(5, truncate=False)
print("ACS combined rows:", acs_df.count() if acs_df else 0)
```

Result:

- `ACS combined rows: 18518` (tract-level population rows with per-year median income attached).

### 6.3 Normalizing GEOID and joining to crime + tracts

```python
from pyspark.sql.functions import regexp_extract, avg

acs_df = (acs_df
          .withColumnRenamed("Population_Data", "population")
          .withColumnRenamed("Median_Income", "median_income"))

acs_df_norm = acs_df.withColumn(
    "tract_geoid",
    regexp_extract(col("GEO_ID"), r"US(\d{11})", 1)
)

acs_df_norm = acs_df_norm.groupBy("tract_geoid").agg(
    avg("median_income").alias("median_income"),
    avg("population").alias("population")
)

crime_with_tract_sliced = crime_with_tract.filter(crime_with_tract.year == 2022).limit(100000)

crime_demo = crime_with_tract_sliced.join(
    acs_df_norm.select("tract_geoid", "median_income", "population"),
    "tract_geoid",
    "left"
)
```

---

## 7. Shapefiles & Spatial Join (Sedona + GeoPandas)

### 7.1 Why not use `sedona.read.format("shapefile")`?

On the host with Spark 3.5.1 + Sedona 1.6.1 shaded:

- Using:
  ```python
  sedona.read.format("shapefile").load(TRACT_SHAPEFILE_DIR)
  ```
  fails with:
  - `DATA_SOURCE_NOT_FOUND: shapefile`
  - Or missing GeoTools classes.

Root cause:

- Only `sedona-spark-shaded` is on the classpath.
- Matching viz/common/GeoTools jars for this combo aren’t available in Maven Central.

### 7.2 Workaround: GeoPandas → WKT → Sedona

1. **Copy shapefiles from HDFS to namenode local**:

   ```bash
   hdfs dfs -get /raw/census_tracts/* /home/hadoop/census_tracts/
   ```

2. **Copy from namenode to host**:

   On host:

   ```bash
   mkdir -p /home/cosmo/census_tracts
   scp -r hadoop@namenode:/home/hadoop/census_tracts/* /home/cosmo/census_tracts/
   ```

3. **Read shapefile with GeoPandas and convert to Spark**:

   ```python
   import geopandas as gpd
   import os
   from pyspark.sql.functions import concat_ws, col, expr

   TRACT_LOCAL_DIR = "/home/cosmo/census_tracts"
   shp_files = [f for f in os.listdir(TRACT_LOCAL_DIR) if f.lower().endswith(".shp")]
   shp_path = os.path.join(TRACT_LOCAL_DIR, shp_files[0])

   gdf = gpd.read_file(shp_path)
   gdf["wkt"] = gdf.geometry.to_wkt()

   cols_to_keep = ["wkt"]
   for c in ["GEOID", "GEOID10", "GEOID20", "STATEFP", "COUNTYFP", "TRACTCE"]:
       if c in gdf.columns:
           cols_to_keep.append(c)

   gdf_small = gdf[cols_to_keep].copy()
   tracts_df = spark.createDataFrame(gdf_small)
   tracts_df = tracts_df.withColumn("tract_geom", expr("ST_GeomFromWKT(wkt)"))

   cols = tracts_df.columns
   has_geoid = "GEOID" in cols or "GEOID10" in cols or "GEOID20" in cols

   if has_geoid:
       geoid_col = "GEOID" if "GEOID" in cols else "GEOID10" if "GEOID10" in cols else "GEOID20"
       tracts_df = tracts_df.withColumn("tract_id", col(geoid_col))
   else:
       for c in ("STATEFP", "COUNTYFP", "TRACTCE"):
           if c not in cols:
               raise ValueError("Shapefile missing GEOID and components")
       tracts_df = tracts_df.withColumn(
           "tract_id",
           concat_ws("", col("STATEFP"), col("COUNTYFP"), col("TRACTCE"))
       )
   ```

4. **Spatial join crimes to tracts**:

   ```python
   from pyspark.sql.functions import expr, col

   crime_points = final.filter(
       final.Latitude.isNotNull() & final.Longitude.isNotNull()
   ).withColumn(
       "crime_point",
       expr("ST_Point(Longitude, Latitude)")
   )

   crime_with_tract = crime_points.join(
       tracts_df,
       expr("ST_Contains(tract_geom, crime_point)"),
       "left"
   ).withColumnRenamed("tract_id", "tract_geoid")
   ```

For development, we sometimes restricted to a smaller slice:

```python
crime_points = (
    final
    .filter(final.Latitude.isNotNull() & final.Longitude.isNotNull())
    .filter(final.year == 2022)
    .limit(200000)
    .withColumn("crime_point", expr("ST_Point(Longitude, Latitude)"))
)
```

---

## 8. Final Writes & Example Analyses

Final integrated dataset write (HDFS):

```python
(crime_demo.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(OUTPUT_DEMO_SPATIAL)
)

df_check = spark.read.parquet(OUTPUT_DEMO_SPATIAL)
df_check.createOrReplaceTempView("crime_weather_demo")
```

Example SQL analyses include:

- Top crime types for 2022.
- Hourly distribution.
- Weather influence by type.
- Daily incidents time series.
- Weekday × hour heatmap.

(Implemented via a dict of SQL queries and `spark.sql(...)` calls.)

---

## 9. Notes & Future Improvements

- This setup demonstrates:
  - Working multi-VM HDFS cluster.
  - Host Spark 3.5.1 integrating with HDFS.
  - Full ETL: Crime × Weather × Demographics × Tracts.
- Potential v2 work:
  - Use a Sedona version + jars that support `format("shapefile")` natively.
  - Add a checkpoint (write/read Parquet) after the spatial join for reliability.
  - Explore GPU-accelerated ETL with RAPIDS/cuDF for very large runs.
```