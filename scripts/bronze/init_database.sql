/*
==================================================================
  Create Datebase and Schemas
==================================================================
Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exsists.
  If the database exsists, it is dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver', and 'gold'.

WARNING:
  Running this script will drop the entire 'Datawarehouse' database if it exsists.
  all data in the database will bepremanently deleted. Proceed with caution
  and ensure you have proper backups before running this script.

*/
  

--Create Database 'DataWarehouse'

USE master;

--Drop and recreate the 'DataWarehouse' database
IF EXSISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE Datawareshouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO


--Create the 'Datawarehouse' Database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
