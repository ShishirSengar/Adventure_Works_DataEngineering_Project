CREATE VIEW gold.calender
AS
SELECT * FROM
OPENROWSET(
    BULK 'https://storageforaw.dfs.core.windows.net/silver/Calender/',
    FORMAT = 'PARQUET'
)  as QUER1;


-- CREATE VIEW CUSTOMERS
CREATE VIEW gold.customers
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storageforaw.dfs.core.windows.net/silver/Customer/',
            FORMAT = 'PARQUET'
) as QUER1;




-- CREATE VIEW PRODUCTS

CREATE VIEW gold.products
AS

SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storageforaw.dfs.core.windows.net/silver/Products/',
            FORMAT = 'PARQUET'
) as QUER1;



-- CREATE VIEW RETURNS

CREATE VIEW gold.returns
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storageforaw.dfs.core.windows.net/silver/Returns/',
            FORMAT = 'PARQUET'
) as QUER1;



-- CREATE VIEW SALES

CREATE VIEW gold.sales
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storageforaw.dfs.core.windows.net/silver/Sales/',
            FORMAT = 'PARQUET'
) as QUER1;



-- CREATE VIEW SUBCAT

CREATE VIEW gold.subcat
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storageforaw.dfs.core.windows.net/silver/Product_SubCategory/',
            FORMAT = 'PARQUET'
) as QUER1;




-- CREATE VIEW TERRITORIES

CREATE VIEW gold.territories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storageforaw.dfs.core.windows.net/silver/Territories/',
            FORMAT = 'PARQUET'
) as QUER1;