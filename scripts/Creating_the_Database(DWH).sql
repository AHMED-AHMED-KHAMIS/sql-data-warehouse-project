/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DWH'

First things first: You should check whether the database name has already been taken or not.

Second and foremost: Since we chose the Medallion Architecture approach, we will create three schemas:

A schema for the Bronze layer

A schema for the Silver layer

A schema for the Gold layer

Hold on a second... what are the other Data Warehouse approaches?

Inmon Approach (also known as the Enterprise Data Warehouse):
Starts with → Source System → Staging → EDW → Data Marts → Reporting.

Kimball Approach (requires less time and money than Inmon’s):
Starts with → Source System → Staging → Data Marts → Reporting.

Data Vault:
Starts with → Source System → Staging → Raw Vault → Business Vault → Data Marts → Reporting.

Medallion Approach (the one we chose for this project):
Starts with → Source System → Bronze Layer → Silver Layer → Gold Layer → Reporting.
so know you can understand why we need to Create 3 schemas 

Just keep in mind: No single approach is the best for all cases—each business case requires a specific solution.
*/
USE master;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DWH;
GO

USE DWH;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
