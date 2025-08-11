-- Active: 1754746877426@@localhost@5434@postgres
-- =============================================================================
-- Database Initialization Script for TRX Journey ELT Pipeline
-- =============================================================================
-- This script creates the necessary databases for the ELT pipeline:
-- 1. dwh_finance - Data Warehouse database for final marts
-- 2. staging_finance - Staging database for intermediate data
-- =============================================================================

-- Create Data Warehouse Database
CREATE DATABASE dwh_finance;

-- Create Staging Database
CREATE DATABASE staging_finance;

-- Grant permissions to postgres user
GRANT ALL PRIVILEGES ON DATABASE dwh_finance TO postgres;
GRANT ALL PRIVILEGES ON DATABASE staging_finance TO postgres;
