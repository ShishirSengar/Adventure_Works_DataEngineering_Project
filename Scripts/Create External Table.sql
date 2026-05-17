-- CREATE MASTER KEY ENCRYPTION BY PASSWORD ='HelloManager@123' 

CREATE DATABASE SCOPED CREDENTIAL cred_shishir
WITH
    IDENTITY = 'Managed Identity'


CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://storageforaw.dfs.core.windows.net/silver',
    CREDENTIAL = cred_shishir
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://storageforaw.dfs.core.windows.net/gold',
    CREDENTIAL = cred_shishir
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

-- CREATE EXTERNAL TABLE EXTSALES
CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS
SELECT * FROM gold.sales

SELECT * from gold.extsales

