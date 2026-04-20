# vic-ev-analytics
## Problem Description

Victoria’s vehicle registration data contains a large and growing number of vehicles, but the raw records are not immediately useful for understanding the shift to electric mobility. The main challenge is that the data is stored in low-level registration fields such as make, model, body type, and fuel-related text, which makes it difficult to reliably identify whether a vehicle is an EV, PHEV, HEV, or traditional ICE vehicle.

This project solves that problem by transforming raw registration data into a clean analytical model that can classify vehicles by powertrain type and support EV adoption analysis over time. Without this transformation, it is hard to answer basic questions such as: How many registered vehicles are electric? Which makes and models are driving EV growth? Is EV adoption increasing month by month? By building lookup tables, staging models, and a final mart, the project turns messy registration data into a reliable dataset for EV market analysis.

The goal is to create a reproducible pipeline that can be used to monitor vehicle registrations, compare EVs against non-EVs, and track adoption trends in a way that is transparent and easy to update. This makes the data useful not just for reporting, but for understanding the real pace of transition in Victoria’s vehicle fleet

## Project Overview

## Data Sources

## Pipeline Design

## Key Charts

## How to Reproduce

flowchart LR
    A[Raw Victorian Registration Data<br/>BigQuery: ext_monthly_vehicle_registration_raw] --> B[dbt Staging Models<br/>stg_registrations]
    C[dbt Seed Tables<br/>lk_make_map, lk_model_map,<br/>lk_ev_classification_clean] --> D[dbt Intermediate Models<br/>int_registrations_enriched]

    B --> D
    D --> E[dbt Mart Models<br/>mart_vehicle_registrations<br/>mart_ev_growth_rate<br/>mart_ev_by_make<br/>mart_ev_share_by_month<br/>mart_top_models]

    E --> F[Looker Studio Dashboard]