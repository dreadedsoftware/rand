-- Databricks notebook source
-- MAGIC %md # System Tables Field Quickstart

-- COMMAND ----------

-- DBTITLE 1,Preliminaries
CREATE WIDGET TEXT tag DEFAULT "environment";
CREATE WIDGET TEXT interval_hours DEFAULT "3";
CREATE WIDGET TEXT discount_percentage DEFAULT "5";

-- COMMAND ----------

-- MAGIC %md ## Overview
-- MAGIC This is not a comprehensive notebook around all the System Tables features. For a more in-depth review of the topic, please find your way to [this series of blog posts](https://medium.com/@wesbert/databricks-system-tables-overview-its-about-answering-questions-f1c22f190275).

-- COMMAND ----------

-- MAGIC %md ## Billing

-- COMMAND ----------

SELECT DISTINCT custom_tags[':tag'] FROM system.billing.usage

-- COMMAND ----------

-- DBTITLE 1,small test to get the hang of things
SELECT *
FROM system.billing.usage
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,All recent and current prices
SELECT *
FROM system.billing.list_prices
WHERE price_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
   OR price_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
   OR price_end_time IS NULL

-- COMMAND ----------

-- DBTITLE 1,DBUs by Cluster
SELECT usage_metadata.cluster_id, SUM(usage_quantity) AS DBUs
FROM system.billing.usage
WHERE (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
   OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
   AND usage_metadata.cluster_id IS NOT NULL
   AND usage_unit = 'DBU'
GROUP BY (usage_metadata.cluster_id)
ORDER BY 2 DESC

-- COMMAND ----------

-- DBTITLE 1,DBUs by Tag
SELECT tag, SUM(DBUs) AS DBUs
FROM (
  SELECT CONCAT(:tag, '=', IFNULL(custom_tags[':tag'], 'null')) AS tag, usage_quantity AS DBUs
  FROM system.billing.usage
  WHERE (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
    OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
    AND usage_metadata.cluster_id IS NOT NULL
    AND usage_unit = 'DBU')
GROUP BY (tag)
ORDER BY 2 DESC

-- COMMAND ----------

-- DBTITLE 1,DBUs by SKU for Classic Compute
SELECT sku_name, SUM(usage_quantity) AS DBUs
FROM system.billing.usage
WHERE (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
   OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
   AND usage_metadata.cluster_id IS NOT NULL
   AND usage_unit = 'DBU'
GROUP BY (sku_name)
ORDER BY 2 DESC

-- COMMAND ----------

-- DBTITLE 1,DBUs by SKU for Serverless SQL
SELECT sku_name, SUM(usage_quantity) AS DBUs
FROM system.billing.usage
WHERE (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
   OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
  AND usage_unit = 'DBU'
  AND sku_name LIKE '%SERVERLESS_SQL%'
GROUP BY (sku_name)
ORDER BY 2 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: prices in system tables are list prices and any discounts are factored in separately

-- COMMAND ----------

-- DBTITLE 1,Recent Cluster Costs in $USD
-- Note: One can always add the workspace_id column to split these numbers by workspace if necessary
WITH current_pricing AS (
  SELECT sku_name, price_start_time, price_end_time, pricing.effective_list.default * (1.000 - (:discount / 100.000)) AS price_with_discount
  FROM system.billing.list_prices
  WHERE usage_unit='DBU'
    AND currency_code='USD'
    AND (price_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
    OR price_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
    OR price_end_time IS NULL)
),
current_usage AS (
  SELECT sku_name, usage_start_time, usage_end_time, usage_quantity AS DBUs
  FROM system.billing.usage
  WHERE (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
    OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
    AND usage_metadata.cluster_id IS NOT NULL
    AND usage_unit = 'DBU'
),
current_usage_with_pricing AS (
  SELECT
    p.sku_name,
    UNIX_MILLIS(p.price_start_time) AS price_start_time,
    UNIX_MILLIS(p.price_end_time) AS price_end_time,
    UNIX_MILLIS(u.usage_start_time) AS usage_start_time,
    UNIX_MILLIS(u.usage_end_time) AS usage_end_time,
    u.DBUs,
    p.price_with_discount
  FROM   current_pricing p
    JOIN current_usage u
    ON p.sku_name = u.sku_name
      AND (CASE
        WHEN p.price_end_time IS NOT NULL THEN
          (u.usage_start_time BETWEEN p.price_start_time AND p.price_end_time
          OR u.usage_end_time BETWEEN p.price_start_time AND p.price_end_time)
        ELSE
          (u.usage_start_time >= p.price_start_time
          OR u.usage_end_time >= p.price_start_time)
        END
      )
)
SELECT SUM(((ARRAY_MIN(ARRAY(usage_end_time, price_end_time)) - ARRAY_MAX(ARRAY(usage_start_time, price_start_time))) / (usage_end_time-usage_start_time)) * DBUs * price_with_discount) AS cost
FROM current_usage_with_pricing

-- COMMAND ----------

-- DBTITLE 1,take a look at the query history table
SELECT * FROM system.query.history LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,%DBUs by SKU
WITH usage AS (
  SELECT sku_name, usage_quantity
  FROM system.billing.usage
  WHERE usage_unit = 'DBU'
    AND (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
      OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
),
total_usage AS (
  -- OK to cross-join against this, it is a singleton dataset
  SELECT SUM(usage_quantity) AS total
  FROM usage
),
by_sku AS (
  SELECT sku_name, SUM(usage_quantity) AS DBUs
  FROM usage, total_usage
  GROUP BY sku_name
)
SELECT sku_name, DBUs/total AS ratio
FROM by_sku, total_usage
ORDER BY 2 DESC

-- COMMAND ----------

-- DBTITLE 1,DLT Maintenance Usage in DBUs
SELECT SUM(usage_quantity) dlt_maintenance_dbus
FROM system.billing.usage
WHERE usage_unit = 'DBU'
  AND (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
    OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
  AND usage_metadata.dlt_maintenance_id IS NOT NULL

-- COMMAND ----------

-- DBTITLE 1,Warehouse Usage in DBUs
SELECT usage_metadata.warehouse_id, SUM(usage_quantity) warehouse_dbus
FROM system.billing.usage
WHERE usage_unit = 'DBU'
  AND (usage_start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
    OR usage_end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
  AND usage_metadata.warehouse_id IS NOT NULL
GROUP BY usage_metadata.warehouse_id
ORDER BY warehouse_dbus DESC

-- COMMAND ----------

-- MAGIC %md ## Informational

-- COMMAND ----------

-- DBTITLE 1,take a look at the node table
SELECT * FROM system.compute.node_types LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Which node types have certain characteristics?
SELECT
  node_type,
  memory_mb/1024 AS memory_gb,
  core_count,
  gpu_count
FROM system.compute.node_types
WHERE
  -- Memory from 64GB to 512GB
  memory_mb/1024 BETWEEN 64 AND 513
  AND core_count > 16
ORDER BY
  -- Some metric to track approx cost
  core_count * memory_mb * (1+gpu_count) DESC

-- COMMAND ----------

SELECT * FROM system.compute.clusters LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Largest Clusters and their Owners
WITH clusters AS (
  SELECT *
  FROM system.compute.clusters
  WHERE delete_time IS NULL -- current
    OR create_time >= (current_timestamp() - make_dt_interval(0, :interval_hours)) -- recent
  QUALIFY 1 = (ROW_NUMBER() OVER (
    PARTITION BY cluster_id
    ORDER BY
      change_time DESC
  ))
),
drivers AS (
  SELECT c.owned_by, c.cluster_id, nt.core_count, nt.memory_mb/1024 AS memory_gb, nt.gpu_count
  FROM system.compute.node_types nt,
       clusters c
  WHERE nt.node_type = c.driver_node_type
),
workers AS (
  SELECT c.owned_by, c.cluster_id, nt.core_count, nt.memory_mb/1024 AS memory_gb, nt.gpu_count, COALESCE(c.worker_count, c.max_autoscale_workers) AS qty
  FROM system.compute.node_types nt,
       clusters c
  WHERE nt.node_type = c.worker_node_type
)
SELECT DISTINCT
  drivers.owned_by,
  drivers.cluster_id,
  drivers.core_count + (workers.core_count * workers.qty) AS total_cores,
  drivers.memory_gb + (workers.memory_gb * workers.qty) AS total_memory_gb,
  drivers.gpu_count + (workers.gpu_count * workers.qty) AS total_gpus
FROM drivers, workers
WHERE drivers.cluster_id = workers.cluster_id
ORDER BY total_cores*total_memory_gb*(1+total_gpus) DESC

-- COMMAND ----------

-- DBTITLE 1,What is the minimum DBR in use and by whom?
WITH dbrs AS (
  SELECT regexp_extract(dbr_version, '(\\d+\\.\\d+\\.(\\d|\\w)+)') AS dbr
  FROM system.compute.clusters
  WHERE delete_time IS NULL -- current
    OR create_time >= (current_timestamp() - make_dt_interval(0, :interval_hours)) -- recent
),
min_dbr AS(
  SELECT MIN(dbr) AS dbr
  FROM dbrs
  WHERE dbr IS NOT NULL
    AND '' <> dbr
)
SELECT DISTINCT owned_by, dbr
FROM system.compute.clusters, min_dbr
WHERE dbr_version LIKE CONCAT('%',dbr,'%')

-- COMMAND ----------

-- DBTITLE 1,Query rates by status
WITH totals AS (
  SELECT execution_status, COUNT(*) qty
  FROM system.query.history
  WHERE execution_status IN ('FINISHED','CANCELED','FAILED') -- Completed only
    AND (start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
      OR end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
  GROUP BY execution_status
),
total_runs AS (
  SELECT SUM(qty) AS total
  FROM totals
)
SELECT execution_status, qty/total AS ratio
FROM totals, total_runs

-- COMMAND ----------

-- DBTITLE 1,Failure reasons
WITH errors AS (
  SELECT
    COALESCE(
      NULLIF(REGEXP_EXTRACT(error_message, '\\[(.*?)\\]'), ''),
      NULLIF(REGEXP_EXTRACT(error_message, '(\\w+((Exception)|(Error)):)'), ''),
      error_message
    ) AS error_type
  FROM system.query.history
  WHERE error_message IS NOT NULL
    AND (start_time >= current_timestamp() - make_dt_interval(0, :interval_hours)
      OR end_time >= current_timestamp() - make_dt_interval(0, :interval_hours))
)
SELECT error_type, COUNT(*) AS qty
FROM errors
GROUP BY error_type
ORDER BY qty DESC

-- COMMAND ----------

-- DBTITLE 1,Clusters that do not bear the owner's name
SELECT cluster_id, cluster_name, owned_by
FROM system.compute.clusters
WHERE cluster_name IS NOT NULL -- clusters with names
  AND owned_by IS NOT NULL -- clusters with Owners
  AND NOT REGEXP(LOWER(cluster_name), 'job-\\d+-run-\\d+.*') -- ignore jobs
  AND NOT REGEXP(
    REPLACE(LOWER(cluster_name), ' '),
    REPLACE(SUBSTR(LOWER(owned_by) FROM 1 FOR INSTR(owned_by, '@')-1), '.', '.?')
  )

-- COMMAND ----------

-- DBTITLE 1,take a look at the audit table
SELECT * FROM system.access.audit LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Job triggers by creator and type
SELECT
  request_params['runCreatorUserName'] AS creator,
  request_params['jobTriggerType'] AS trigger,
  COUNT(*) AS qty
FROM system.access.audit
WHERE service_name = 'jobs'
  AND action_name = 'runTriggered'
  AND event_time >= (current_timestamp() - make_dt_interval(0, :interval_hours))
GROUP BY all
ORDER BY qty DESC

-- COMMAND ----------

-- DBTITLE 1,MAUs
SELECT COUNT(DISTINCT user_identity)
FROM system.access.audit
WHERE event_time >= (current_timestamp() - make_dt_interval(0, :interval_hours))
  AND user_identity.email LIKE '%@%'

-- COMMAND ----------

-- DBTITLE 1,Failure leaders
SELECT user_identity.email AS email, COUNT(*) AS qty
FROM system.access.audit
WHERE event_time >= (current_timestamp() - make_dt_interval(0, :interval_hours))
  AND user_identity.email LIKE '%@%'
  AND NOT (response.status_code BETWEEN 200 AND 300)
GROUP BY ALL
ORDER BY qty DESC

-- COMMAND ----------

-- DBTITLE 1,Successful DB SQL downloads by hour
SELECT DATE_FORMAT(event_time, 'yyyyMMddHH') AS the_hour, COUNT(*) AS qty
FROM system.access.audit
WHERE event_time >= (current_timestamp() - make_dt_interval(0, :interval_hours))
  AND service_name = 'databrickssql'
  AND action_name = 'downloadQueryResult'
  AND response.status_code = 200
GROUP BY ALL
ORDER BY the_hour DESC

-- COMMAND ----------

