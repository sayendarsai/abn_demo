%sql
-- ================================
-- 1. CONFIGURATION
-- ================================
CREATE CATALOG IF NOT EXISTS abn_demo;

USE CATALOG abn_demo;

-- ================================
-- 2. BRONZE LAYER
-- ================================
CREATE SCHEMA IF NOT EXISTS bronze_abn;

CREATE TABLE IF NOT EXISTS bronze_abn.episodes
USING DELTA;

CREATE TABLE IF NOT EXISTS bronze_abn.cast
USING DELTA;

-- ================================
-- 3. SILVER LAYER
-- ================================
CREATE SCHEMA IF NOT EXISTS silver_abn;

CREATE TABLE IF NOT EXISTS silver_abn.shows
USING DELTA;

CREATE TABLE IF NOT EXISTS silver_abn.episodes
USING DELTA;

CREATE TABLE IF NOT EXISTS silver_abn.cast
USING DELTA;

CREATE TABLE IF NOT EXISTS silver_abn.fact_table
USING DELTA;

-- ================================
-- 4. GOLD LAYER
-- ================================
CREATE SCHEMA IF NOT EXISTS gold_abn;

CREATE TABLE IF NOT EXISTS gold_abn.episodes_per_season
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_abn.avg_runtime
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_abn.top_cast
USING DELTA;

CREATE TABLE IF NOT EXISTS gold_abn.common_genres
USING DELTA;
