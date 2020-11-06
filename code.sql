-- step 3



CREATE LOGIN vitalii 
	WITH PASSWORD = '<password>' 
GO



CREATE SCHEMA vitalii_schema
GO



CREATE USER vitalii
	FOR LOGIN vitalii
	WITH DEFAULT_SCHEMA = vitalii_schema
GO

EXEC sp_addrolemember N'db_owner', N'vitalii'
GO


-- step 5



CREATE DATABASE SCOPED CREDENTIAL AzureStorageVitalii
WITH
  IDENTITY = '<name>' ,
  SECRET = '<key>' ;

-- Create an external data source with CREDENTIAL option.
CREATE EXTERNAL DATA SOURCE vitalii_blob
WITH
  ( LOCATION = 'wasbs://homework2@bigdataschoolstr98.blob.core.windows.net/' ,
    CREDENTIAL = AzureStorageVitalii ,
    TYPE = HADOOP
  ) ;
GO



-- step 6



CREATE EXTERNAL FILE FORMAT file_format_vitalii WITH
(
	FORMAT_TYPE = DELIMITEDTEXT,
	FORMAT_OPTIONS 
	(
		FIELD_TERMINATOR = N',',
		USE_TYPE_DEFAULT = False,
		FIRST_ROW = 2  -- Optional property
	)
)
GO



CREATE EXTERNAL TABLE vitalii_schema.ext_table
(
	[VendorID] [int] NULL,
	[tpep_pickup_datetime] [datetime] NULL,
	[tpep_dropoff_datetime] [datetime] NULL,
	[passenger_count] [int] NULL,
	[Trip_distance] [real] NULL,
	[RatecodeID] [int] NULL,
	[store_and_fwd_flag] [char](1) NULL,
	[PULocationID] [int] NULL,
	[DOLocationID] [int] NULL,
	[payment_type] [int] NULL,
	[fare_amount] [real] NULL,
	[extra] [real] NULL,
	[mta_tax] [real] NULL,
	[tip_amount] [real] NULL,
	[tolls_amount] [real] NULL,
	[improvement_surcharge] [real] NULL,
	[total_amount] [real] NULL,
	[congestion_surcharge] [real] NULL
)
WITH
(
    DATA_SOURCE = vitalii_blob,
	LOCATION = N'yellow_tripdata_2020-01.csv',
    FILE_FORMAT = file_format_vitalii,
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
)
GO



-- step 7



CREATE TABLE vitalii_schema.fact_tripdata
(
	[VendorID] [int] NULL,
	[tpep_pickup_datetime] [datetime] NULL,
	[tpep_dropoff_datetime] [datetime] NULL,
	[passenger_count] [int] NULL,
	[Trip_distance] [real] NULL,
	[RatecodeID] [int] NULL,
	[store_and_fwd_flag] [char](1) NULL,
	[PULocationID] [int] NULL,
	[DOLocationID] [int] NULL,
	[payment_type] [int] NULL,
	[fare_amount] [real] NULL,
	[extra] [real] NULL,
	[mta_tax] [real] NULL,
	[tip_amount] [real] NULL,
	[tolls_amount] [real] NULL,
	[improvement_surcharge] [real] NULL,
	[total_amount] [real] NULL,
	[congestion_surcharge] [real] NULL
)
WITH
(   
    DISTRIBUTION = HASH (tpep_pickup_datetime),
    CLUSTERED COLUMNSTORE INDEX
)
GO

INSERT INTO vitalii_schema.fact_tripdata 
    SELECT *  
    FROM vitalii_schema.ext_table;  
GO  



-- step 8



CREATE TABLE vitalii_schema.Vendor
(
	[ID] [int] NULL,
	[Name] [varchar](255) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED COLUMNSTORE INDEX
)
GO

INSERT INTO vitalii_schema.Vendor (ID)
	SELECT DISTINCT VendorID FROM vitalii_schema.fact_tripdata WHERE VendorID IS NOT NULL; 
GO  

UPDATE vitalii_schema.Vendor 
SET [Name] = CASE ID
                      WHEN 1 THEN 'Creative Mobile Technologies, LLC' 
                      WHEN 2 THEN 'VeriFone Inc.' 
                      ELSE [Name]
                      END
GO



CREATE TABLE vitalii_schema.RateCode
(
	[ID] [int] NULL,
	[Name] [varchar](255) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED COLUMNSTORE INDEX
)
GO

INSERT INTO vitalii_schema.RateCode (ID)
	SELECT DISTINCT RateCodeID FROM vitalii_schema.fact_tripdata WHERE RateCodeID IS NOT NULL; 
GO  

UPDATE vitalii_schema.RateCode 
SET [Name] = CASE ID
                      WHEN 1 THEN 'Standard rate' 
                      WHEN 2 THEN 'JFK' 
					  WHEN 3 THEN 'Newark' 
					  WHEN 4 THEN 'Nassau or Westchester' 
					  WHEN 5 THEN 'Negotiated fare' 
					  WHEN 6 THEN 'Group ride' 
                      ELSE [Name]
                      END
GO



CREATE TABLE vitalii_schema.Payment_type
(
	[ID] [int] NULL,
	[Name] [varchar](255) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED COLUMNSTORE INDEX
)
GO

INSERT INTO vitalii_schema.Payment_type (ID)
	SELECT DISTINCT Payment_type FROM vitalii_schema.fact_tripdata WHERE Payment_type IS NOT NULL; 
GO  

UPDATE vitalii_schema.Payment_type 
SET [Name] = CASE ID
                      WHEN 1 THEN 'Credit card' 
                      WHEN 2 THEN 'Cash' 
					  WHEN 3 THEN 'No charge' 
					  WHEN 4 THEN 'Dispute' 
					  WHEN 5 THEN 'Unknown' 
					  WHEN 6 THEN 'Voided trip' 
                      ELSE [Name]
                      END
GO