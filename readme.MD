
# Part 1: Data Platform Architecture on AWS

Design a scalable, maintainable, and future-ready data platform on AWS for Pave.io, supporting both batch and real-time processing, with integration-ready paths for advanced analytics and machine learning.

---

## 1. Solution Justification

### 1.1 Overview
We propose a lightweight yet robust architecture that is scalable, cost-effective, and easy to maintain. The design enables seamless integration with future machine learning initiatives and supports both structured and semi-structured data workflows.

### 1.2 Data Ingestion

- **Proactive Push (IoT/Mobile)**: Utilize **Amazon Kinesis Data Streams** to capture real-time events pushed from mobile apps or IoT devices. Stream data using **Amazon Kinesis Data Analytics (Flink)** from source to sink (Amazon S3).

- **Scheduled Pull (REST APIs)**: Use **AWS Lambda** triggered by **Amazon EventBridge** to periodically extract data from external APIs.

- **Callback APIs**: Handle inbound REST API callbacks via **Amazon API Gateway** integrated with **Lambda**. Forward the payload to **Kinesis** and process in real-time using **Flink**, persisting output to **Amazon S3**.

### 1.3 Data Lake Design

We implement a centralized data lake using **Amazon S3**, which acts as the landing and staging layer. This decouples raw data ingestion from downstream structured processing and enables future use cases like ML model training.

#### 1.3.1 Metadata Management
To handle structured and semi-structured files on S3 (without native DDL), we use **AWS Glue Data Catalog** for schema discovery, classification, and querying with **Amazon Athena**, **AWS Glue**, or **Redshift Spectrum**.

#### 1.3.2 Ad-hoc Querying
Use **Amazon Athena** for querying data directly from S3. Athena can also serve as a data source for BI tools like **Amazon QuickSight** or **Power BI**.

#### 1.3.3 Ad-hoc Data Transformations
Use **AWS Glue ETL Jobs** to perform Spark-based transformations, writing the output back to S3.

#### 1.3.4 Lakehouse Architecture
Implement a lakehouse using **Apache Iceberg** on S3 with **AWS Glue** and **Athena**, enabling ACID transactions and versioned tables.

#### 1.3.5 Data Analytics
Use **Amazon SageMaker** for ML workloads, connecting directly to S3 or querying via the Glue Data Catalog.

### 1.4 Data Warehouse

While the data lake supports flexibility and raw data storage, we also implement a structured **data warehouse on Amazon Redshift** for governed, performant analytics and BI consumption.

#### 1.4.1 Loading Data into Redshift
Use **S3 Event Notifications + Amazon SQS + Lambda** to trigger copy jobs. Simplify ingestion by predefining generic staging tables with `VARCHAR` columns and loading raw data directly. This enables a unified, code-reusable ingestion process without per-table customization.

#### 1.4.2 Physical Warehouse Layer
**Amazon Redshift** is AWS’s native data warehousing solution. (Note: **ClickHouse Cloud** on AWS Marketplace is a potential alternative for high-performance analytics at potentially lower cost.)

#### 1.4.3 Data Modeling and Pipeline Design
Implement a 3-tier model in Redshift:

- `raw`: Raw data copied from S3  
- `staging`: Enriched and cleaned data  
- `marts`: Star-schema modeled tables for analytics  

Follow **Kimball Dimensional Modeling** standards, with fact and dimension tables tailored for self-service BI.

#### 1.4.4 Data Transformation (ELT)
Modern ELT favors in-warehouse SQL-based transformation over external ETL processes. We propose using **dbt (Data Build Tool)** to orchestrate and manage transformations in Redshift.

- dbt enables SQL-based pipelines.
- Business Analysts (BAs) and Data Analysts (DAs) can participate directly in transformation logic.
- CI/CD ready: transformation logic can be versioned and deployed using GitOps.
- Deploy dbt on **AWS Fargate** using:  
  `dbt seed && dbt run && dbt artifacts`

#### 1.4.5 Data Validation
- Use **dbt tests** for schema validation, regex checks, null detection, uniqueness, and freshness checks.
- Validation results are persisted in Redshift and can be visualized or monitored.

### 1.5 BI & Visualization

- Use **Amazon QuickSight** for serverless BI. However, due to limitations (e.g., lack of virtual joins, complex measures, advanced charting), consider **Power BI**, **Tableau**, or **Apache Superset** for more advanced use cases.
- When using QuickSight, pre-join fact and dimension tables via Redshift views to compensate for modeling limitations.
- Demo dashboards are built using **Power BI**, which offers advanced features such as DAX, Copilot AI assistant, and dynamic visualizations.

### 1.6 Pipeline Orchestration
Use **AWS Step Functions** to orchestrate ETL/ELT processes and manage task dependencies.

### 1.7 Data Catalog & Governance
Maintain a unified metadata layer using **dbt docs**, **dbt tests**, and profiling. For advanced governance, consider deploying **OpenMetadata** on **EC2**, **Fargate**, or **EKS** to manage:

- Business Glossary  
- Tagging & Data Lineage  
- Data Profiling  
- Schema Evolution Tracking  

### 1.8 Real-time Monitoring
Use **Amazon Managed Grafana** to query Redshift/Athena and display real-time data freshness and data quality dashboards.

### 1.9 Notification Services
Use **Amazon SNS** to send alerts or pipeline failure notifications.

### 1.10 Logging & Observability
Use **Amazon CloudWatch** to log Lambda functions, API Gateway calls, and ETL/ELT job statuses.

### 1.11 CI/CD & Deployment
Use **GitLab CI/CD** in combination with **Terraform** or **Teraflow** to automate infrastructure and dbt deployments.

### 1.12 Security
Implement AWS security best practices:

- Use **IAM roles** for least privilege access  
- Store secrets in **AWS Secrets Manager** or **Key Management Service (KMS)**  
- Apply **data masking** or tokenization for sensitive fields

---

## 2. Architecture Diagram

The architecture is visualized below based on the components discussed:

![Image](images/Pave_io.drawio.png "Pave.io - Data Platform")

---

# Part 2: Data Transformation & Modeling

## 2.1 Data Quality Checks

### Validate VIN Format
**SQL Rule:**

```sql
SELECT *
FROM {{ ref('fact_inspection') }}
WHERE vehicle_vin IS NULL
   OR LENGTH(vehicle_vin) != 17
   OR vehicle_vin ~ '[^A-Z0-9]';
```

**dbt Test:**

```yaml
- name: fact_inspection
  tests:
    - dbt_expectations.expect_column_values_to_match_regex:
        column: vehicle_vin
        regex: '^[A-Z0-9]{17}$'
```

### Check Missing Fields
See: `vld_missing_fields.sql`

### Identify Duplicates
See: `vld_duplication.sql`

### Flag Anomalous Durations
See: `vld_anomalous_durations.sql`

---

## 2.2 Dimensional Modeling (Star Schema)

Create fact and dimension tables based on business requirements, starting with an **Enterprise Bus Matrix (EBM)** to align dimensions and facts.

![Dimensional Reports](images/dimensional_reports.jpg "Star Schema Design")

### Fact Tables
#### Fact Inspection
- Dim Date
- Dim Vehicles
- Dim Inspector
- Dim Region
- Inspection Duration

Check code at: [pv_dwh/models/marts/fact/fact_inspection.sql](pv_dwh/models/marts/fact/fact_inspection.sql)

#### Fact Inspection Damage
- Dim Date
- Dim Vehicles
- Dim Inspector
- Dim Region
- Damage Cost

Check code at: [pv_dwh/models/marts/fact/fact_inspection_damage.sql](pv_dwh/models/marts/fact/fact_inspection_damage.sql)

### Dimension Tables
- **Dim Date** – [pv_dwh/models/marts/dim/dim_date.sql](pv_dwh/models/marts/dim/dim_date.sql)
- **Dim Vehicles** – [pv_dwh/models/marts/dim/dim_vehicle.sql](pv_dwh/models/marts/dim/dim_vehicle.sql)
- **Dim Inspector (SCD Type 2)** – [pv_dwh/models/marts/dim/dim_inspector.sql](pv_dwh/models/marts/dim/dim_inspector.sql)
- **Dim Region** – [pv_dwh/models/marts/dim/dim_region.sql](pv_dwh/models/marts/dim/dim_region.sql)

- In case dim region, we need to build region (state / city) by reverse from coordination value from inspection data, and create new table in dim_location with location, city, state

```
with open(output_file, mode='a', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=["lat", "lon", "city_or_county", "state"])

    for lat, lon in coords:
        try:
            url = f"https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat={lat}&lon={lon}&addressdetails=1"
            headers = {"User-Agent": "geo-reverse-agent"}
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()

            address = r.json().get("address", {})
            city = (
                address.get("city")
                or address.get("town")
                or address.get("village")
                or address.get("county")
                or "Unknown"
            )
            state = address.get("state", "Unknown")

            row = {"lat": lat, "lon": lon, "city_or_county": city, "state": state}
            writer.writerow(row)
            print(f"✓ {lat},{lon} -> {city}, {state}")

        except Exception as e:
            print(f"✗ Error at {lat},{lon}: {e}")

        time.sleep(0.5)
```

### Build the models and run

- 1. Install DBT and DBT for Redshift

```
pip install dbt-core dbt-redshift
```

- 2. Install dbt dependencies

```
dbt deps
```

- 3. Run dbt seed to load dataset

```
dbt run --profile pv_dwh --target raws --profiles-dir profiles
```

- 4. Run dbt for making enriched data

```
dbt run --profile pv_dwh --target transitives --profiles-dir profiles --select models/transitives
```

- 5. Run dbt to build final data marts

```
dbt run --profile pv_dwh --target marts --profiles-dir profiles --select models/marts
```


## 2.2 Implement Slowly Changing Dimensions (SCD) for Inspector Data

Following the **Type 2 Slowly Changing Dimensions** methodology as described in *The Data Warehouse Toolkit* by Kimball (Chapter 5), we consider two scenarios:

### Case I: No Change Detected  
If the incoming data matches the existing record in the `dim_inspector` table, no action is required.

### Case II: Change Detected  
If there are differences between the new data and the existing record:

- Insert a new row into `dim_inspector` capturing the updated attributes, along with `valid_from` and `valid_to` fields to track the effective period.
- Use a surrogate key `sk_inspector_id` (typically a composite of `inspector_id` and `valid_from`) instead of relying solely on `inspector_id`.
- Reference this surrogate key (`sk_inspector_id`) in both `fact_inspection` and `fact_inspection_damage` tables to ensure proper historical tracking.

There are two types of new entries:
1. **New Inspector Record** (not previously present in the dimension table)  
2. **Modified Existing Record** (attributes have changed compared to the latest version)

For new records, the logic is as follows:

```
WITH snapshot AS (
    SELECT * FROM raw.inspectors i 
),
current AS (
    SELECT * FROM marts.dim_inspector_scd
)
SELECT 
    s.inspector_id,
    s.name,
    s.region,
    s.experience_level,
    CAST(s.certification_date AS DATE) AS certification_date,
    s.inspections_completed,
    '2000-01-01' AS valid_from,
    '3000-12-31'::timestamp AS valid_to,
    NOW() AS last_data_changed_time
    FROM snapshot s
    LEFT JOIN current c ON s.inspector_id = c.inspector_id
    WHERE c.inspector_id IS null;
```

For Modified Existing Record:

```
SELECT
    r.inspector_id,
    r.name,
    r.region,
    r.experience_level,
    CAST(r.certification_date AS DATE) AS certification_date,
    r.inspections_completed,
    NOW() AS valid_from,
    '3000-12-31'::timestamp AS valid_to,
    NOW() AS last_data_changed_time
FROM raw.inspectors r
JOIN marts.dim_inspector_scd d
    ON r.inspector_id = d.inspector_id
WHERE r.name IS DISTINCT FROM d.name OR
      r.region IS DISTINCT FROM d.region OR
      r.experience_level IS DISTINCT FROM d.experience_level OR
      CAST(r.certification_date AS DATE) IS DISTINCT FROM d.certification_date OR
      r.inspections_completed IS DISTINCT FROM d.inspections_completed
```

Then, merge to existing Dim Inspectors (see code at: [pv_dwh/models/transitives/inspector_full.sql](pv_dwh/models/transitives/inspector_full.sql))

Next, we create sk_inspector_id and adjust the valid_from & valid_to that indicates the changed time of a record

```
WITH ordered_versions AS (
    SELECT
        *,
        LEAD(valid_from) OVER (
            PARTITION BY inspector_id
            ORDER BY valid_from ASC
        ) AS next_valid_from
    FROM transitive.dim_inspector_all_changes
)
SELECT
    inspector_id || '---' || md5(
        inspector_id || '|' || name || '|' || region || '|' ||
        experience_level || '|' || certification_date || '|' ||
        inspections_completed || '|' || valid_from
    ) AS sk_inspector_id,
    inspector_id,
    name,
    region,
    experience_level,
    certification_date,
    inspections_completed,
    valid_from,
    COALESCE(next_valid_from, '9999-12-31'::timestamp) AS valid_to,
    last_data_changed_time
FROM ordered_versions;
```

Finally, update related fact: Fact inspection & Fact inspection damages to point to correct inspector record

```
SELECT
    i.inspection_id,
    d.sk_inspector_id,
    i.vehicle_vin,
    i.inspector_id,
    CAST(i.inspection_date AS timestamp) AS inspection_datetime,
    TO_CHAR(CAST(i.inspection_date AS timestamp), 'YYYYMMDD')::int AS date_key,
    i.status,
    i.duration_minutes,
    i.location_lat,
    i.location_lon
FROM raw.inspections i
LEFT JOIN marts.dim_inspector d
  ON i.inspector_id = d.inspector_id
 AND CAST(i.inspection_date AS timestamp) >= d.valid_from
 AND CAST(i.inspection_date AS timestamp) <  d.valid_to;
```

### - Calculating Derived Metrics

When a well-designed dimensional data model is in place, these metrics can often be derived naturally within BI tools through drag-and-drop interactions with dimensions and measures. However, for clarity and to fulfill requirements, we also provide the SQL implementations:

#### + Average Damage Cost by Vehicle Type
Check code at: [pv_dwh/models/adhoc_analysis/avg_damage_costs.sql](pv_dwh/models/adhoc_analysis/avg_damage_costs.sql)

#### + Inspector Performance Scores
Check code at: [pv_dwh/models/adhoc_analysis/avg_damage_costs.sql](pv_dwh/models/adhoc_analysis/avg_damage_costs.sql)

#### + Geographic Damage Patterns
Check code at: [pv_dwh/models/adhoc_analysis/geographic_heatmap.sql](pv_dwh/models/adhoc_analysis/geographic_heatmap.sql)

#### + Time-based Inspection Trends
Check code at: [pv_dwh/models/adhoc_analysis/monthly_trendings.sql](pv_dwh/models/adhoc_analysis/monthly_trendings.sql)

---

# Part 3: Analytics & Visualization (35 points)

## 1. SQL Analytics Queries

Write optimized SQL queries for the following metrics. While these could be generated via BI tools, we include SQL code here as requested:

- Top 10 most common damage types by vehicle make  
- Inspector efficiency metrics (e.g., inspections per day, accuracy rates)  
- Monthly trends in inspection volumes and damage costs  
- Geographic heatmap of damage severity

## 2. QuickSight Dashboard Mock-up

As noted in Part 1, Amazon QuickSight has limitations with star schema modeling. Therefore, Power BI is used here to demonstrate advanced relationships and visualizations.

### Dashboard Requirements (5+ Visuals):
- KPI Cards for key metrics  
- Line Chart for time series of inspection volumes  
- Map Chart for geographic damage distribution  
- Table Chart for inspector performance leaderboard  
- Pie Chart for damage cost breakdown by category  

### 2.1 Option 1: QuickSight with Pre-joined Data Models (Flattened Fact)

Create a flattened view by joining fact and dimension tables. See code:  
[pv_dwh/models/marts/fact/fact_inspection_damage_flat_v.sql](pv_dwh/models/marts/fact/fact_inspection_damage_flat_v.sql)

This pre-joined view (`fact_inspection_damage_flat_v`) is then used in QuickSight.

![Image](images/quicksight-edit.jpg "Edit Quicksight")  
![Image](images/quicksight-reports.jpg "View Quicksight")

### 2.2 Option 2: Power BI with Star Schema Modeling

Power BI supports native star schema modeling and allows defining relationships directly.

- Define relationships between fact and dimension tables  
![Image](images/pbi1.jpg "PBI")  
![Image](images/pbi2.jpg "PBI")

- Build slicers and charts  
![Image](images/pbi3.jpg "PBI")  
![Image](images/pbi4.jpg "PBI")

- For heatmap by state, create a virtual table to support fill map rendering  
![Image](images/pbi6.jpg "PBI")  
![Image](images/pbi5.jpg "PBI")

Power BI dashboard file is available at:  
[visualization/inspections_data.pbix](visualization/inspections_data.pbix)

---

## 3. Real-time Monitoring Queries

Design SQL queries for operational dashboards using Grafana connected to Amazon Redshift:

- **Live Inspection Count (Last Hour):**  
  [pv_dwh/models/adhoc_analysis/live_inspection_count.sql](pv_dwh/models/adhoc_analysis/live_inspection_count.sql)

- **Queue Depth by Region:**  
  [pv_dwh/models/adhoc_analysis/queue_depth_by_region.sql](pv_dwh/models/adhoc_analysis/queue_depth_by_region.sql)

- **Failed Inspection Alerts:**  
  [pv_dwh/models/adhoc_analysis/failled_alerts.sql](pv_dwh/models/adhoc_analysis/failled_alerts.sql)
