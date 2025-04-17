
-- Create Database and Use It
CREATE OR REPLACE DATABASE RetailSales_DW;
USE DATABASE RetailSales_DW;

-- Create or Replace Schema
CREATE OR REPLACE SCHEMA Staging;
CREATE OR REPLACE SCHEMA DataWarehouse;
CREATE OR REPLACE SCHEMA DataWarehouse;

-- Create GCP Storage Integration in Snowflake
CREATE OR REPLACE STORAGE INTEGRATION my_gcs_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://hari_dw/');

-- Create External Stage for GCS
CREATE OR REPLACE STAGE gcs_stage
URL = 'gcs://hari_dw/'
STORAGE_INTEGRATION = my_gcs_integration
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- Create or Replace Staging Tables

CREATE OR REPLACE TABLE Staging.Dates (
    OrderDate DATE PRIMARY KEY,
    ShipDate DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT
);

CREATE OR REPLACE TABLE Staging.Customers (
    CustomerID STRING PRIMARY KEY,
    CustomerName STRING,
    Segment STRING,
    Country STRING,
    City STRING,
    State STRING,
    PostalCode STRING,
    Region STRING,
    Age INT
);

CREATE OR REPLACE TABLE Staging.Products (
    ProductID STRING PRIMARY KEY,
    Category STRING,
    SubCategory STRING,
    ProductName STRING
);

CREATE OR REPLACE TABLE Staging.Orders (
    OrderID STRING PRIMARY KEY,
    CustomerID STRING,
    ProductID STRING,
    Sales FLOAT,
    Quantity INT,
    Discount FLOAT,
    Profit FLOAT,
    OrderDate DATE,
    ShipDate DATE,
    ShipMode STRING,
    OrderStatus STRING
);

-- Load Data from GCS to Snowflake
COPY INTO Staging.Dates FROM @gcs_stage/Dates.csv FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"') ON_ERROR = CONTINUE;
COPY INTO Staging.Customers FROM @gcs_stage/Customers.csv FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"') ON_ERROR = CONTINUE;
COPY INTO Staging.Products FROM @gcs_stage/Products.csv FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"') ON_ERROR = CONTINUE;
COPY INTO Staging.Orders FROM @gcs_stage/Orders.csv FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='"') ON_ERROR = CONTINUE;

-- Create or Replace Fact & Dimension Tables

CREATE OR REPLACE TABLE DataWarehouse.Dim_Dates (
    DateKey INT AUTOINCREMENT PRIMARY KEY,
    OrderDate DATE UNIQUE,
    ShipDate DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT
);
INSERT INTO DataWarehouse.Dim_Dates (OrderDate, ShipDate, Year, Quarter, Month, Day)
SELECT DISTINCT OrderDate, ShipDate, 
    EXTRACT(YEAR FROM OrderDate), EXTRACT(QUARTER FROM OrderDate), 
    EXTRACT(MONTH FROM OrderDate), EXTRACT(DAY FROM OrderDate)
FROM Staging.Orders;

CREATE OR REPLACE TABLE DataWarehouse.Dim_Customers (
    CustomerKey INT AUTOINCREMENT PRIMARY KEY,
    CustomerID STRING UNIQUE,
    CustomerName STRING,
    Segment STRING,
    Country STRING,
    City STRING,
    State STRING,
    PostalCode STRING,
    Region STRING,
    Age INT
);
INSERT INTO DataWarehouse.Dim_Customers (
    CustomerID, CustomerName, Segment, Country, City, State, PostalCode, Region, Age
) SELECT DISTINCT 
    CustomerID, CustomerName, Segment, Country, City, State, PostalCode, Region, Age 
FROM Staging.Customers;

CREATE OR REPLACE TABLE DataWarehouse.Dim_Products (
    ProductKey INT AUTOINCREMENT PRIMARY KEY,
    ProductID STRING UNIQUE,
    Category STRING,
    SubCategory STRING,
    ProductName STRING
);
INSERT INTO DataWarehouse.Dim_Products (
    ProductID, Category, SubCategory, ProductName
) SELECT DISTINCT 
    ProductID, Category, SubCategory, ProductName 
FROM Staging.Products;

CREATE OR REPLACE TABLE DataWarehouse.Fact_Orders (
    OrderKey INT AUTOINCREMENT PRIMARY KEY,
    OrderID STRING UNIQUE,
    CustomerKey INT REFERENCES DataWarehouse.Dim_Customers(CustomerKey),
    ProductKey INT REFERENCES DataWarehouse.Dim_Products(ProductKey),
    Sales FLOAT,
    Quantity INT,
    Discount FLOAT,
    Profit FLOAT,
    OrderStatus STRING,
    OrderDate DATE
);
INSERT INTO DataWarehouse.Fact_Orders (
    OrderID, CustomerKey, ProductKey, Sales, Quantity, Discount, Profit, OrderStatus, OrderDate
) SELECT 
    o.OrderID, 
    COALESCE(c.CustomerKey, -1), 
    COALESCE(p.ProductKey, -1), 
    o.Sales, 
    o.Quantity, 
    o.Discount, 
    o.Profit,
    o.OrderStatus,
    o.OrderDate
FROM Staging.Orders o
LEFT JOIN DataWarehouse.Dim_Customers c ON o.CustomerID = c.CustomerID
LEFT JOIN DataWarehouse.Dim_Products p ON o.ProductID = p.ProductID;

-- Analytical Queries
-- Total Revenue by Category
SELECT p.Category, SUM(f.Sales) AS Total_Revenue
FROM DataWarehouse.Fact_Orders f
JOIN DataWarehouse.Dim_Products p ON f.ProductKey = p.ProductKey
GROUP BY p.Category
ORDER BY Total_Revenue DESC;

-- Top 5 Customers by Purchases
SELECT c.CustomerName, SUM(f.Sales) AS TotalSpent
FROM DataWarehouse.Fact_Orders f
JOIN DataWarehouse.Dim_Customers c ON f.CustomerKey = c.CustomerKey
GROUP BY c.CustomerName
ORDER BY TotalSpent DESC
LIMIT 8;

-- Quarterly Sales Trends
SELECT EXTRACT(YEAR FROM f.OrderDate) AS Year, EXTRACT(QUARTER FROM f.OrderDate) AS Quarter, SUM(f.Sales) AS Quarterly_Revenue
FROM DataWarehouse.Fact_Orders f
GROUP BY Year, Quarter
ORDER BY Year, Quarter;

-- Customer Segment Performance
SELECT c.Segment, SUM(f.Sales) AS Total_Sales, AVG(f.Profit) AS Avg_Profit
FROM DataWarehouse.Fact_Orders f
JOIN DataWarehouse.Dim_Customers c ON f.CustomerKey = c.CustomerKey
GROUP BY c.Segment
ORDER BY Total_Sales DESC;

-- Most Profitable Products
SELECT p.ProductName, SUM(f.Profit) AS Total_Profit
FROM DataWarehouse.Fact_Orders f
JOIN DataWarehouse.Dim_Products p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName
ORDER BY Total_Profit DESC
LIMIT 15;

-- Sales Distribution by Region
SELECT c.Region, SUM(f.Sales) AS Total_Sales
FROM DataWarehouse.Fact_Orders f
JOIN DataWarehouse.Dim_Customers c ON f.CustomerKey = c.CustomerKey
GROUP BY c.Region
ORDER BY Total_Sales DESC;





