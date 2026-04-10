# Healthcare Analytics Platform

## Problem Statement
Rural hospitals with high Medicaid populations show 
persistently worse readmission rates than urban counterparts,
driving excess Medicare spending across underserved counties.

## Architecture
Medallion Architecture on Azure Databricks
Bronze → Silver → Gold → Power BI

## Data Sources
- CMS Hospital Readmissions Reduction Program
- Medicare Geographic Variation by County
- Provider of Services File

## Key Questions Answered
1. Which hospitals have highest excess readmission ratios?
2. Do rural hospitals perform worse than urban?
3. Which conditions drive the most readmissions?
4. Do high Medicaid counties correlate with worse outcomes?

## Tech Stack
Azure Databricks | Delta Lake | ADLS Gen2 | 
Unity Catalog | PySpark | SQL | Power BI

## Environment Setup
Dev → rg-healthcare-analytics-dev
Prod → rg-healthcare-analytics-prod
