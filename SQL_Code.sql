-- Databricks notebook source
SELECT *
FROM read_files('/Volumes/idp/default/idp_project')

-- COMMAND ----------

CREATE OR REPLACE TABLE parsed_data AS
SELECT path,
ai_parse_document(content) as parsed_content
FROM read_files('/Volumes/idp/default/idp_project')

-- COMMAND ----------

SELECT *
FROM parsed_data

-- COMMAND ----------

CREATE or REPLACE TABLE pretty_data AS
SELECT path,
concat_ws('\n',transform(try_cast(parsed_content:document:elements as array<variant>), e -> coalesce(try_cast(e:content as string), ''))) as doc_text
FROM parsed_data

-- COMMAND ----------

CREATE OR REPLACE TABLE classified_data AS
SELECT *,
ai_classify(doc_text,ARRAY('Invoice','Purchase_Order','Receipt','Other')) as doc_classification
FROM pretty_data

-- COMMAND ----------

CREATE OR REPLACE TABLE invoice_data AS
SELECT *,
ai_extract(doc_text,
ARRAY('Vendor_Name','Invoice_Number','Invoice_Date','Due_Date','Payment_Method','Total')) as extracted
FROM classified_data
WHERE doc_classification = 'Invoice'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS idp.finance

-- COMMAND ----------

CREATE OR REPLACE TABLE idp.finance.invoices AS
SELECT path,
extracted.Vendor_Name AS Vendor,
extracted.Invoice_Number AS Invoice_Number,
extracted.Invoice_Date AS Invoice_Date,
extracted.Due_Date AS Due_Date,
extracted.Payment_Method AS Payment_Method,
extracted.Total AS Total
FROM invoice_data

-- COMMAND ----------

SELECT *
FROM idp.finance.invoices

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_order_data AS
SELECT *,
ai_extract(doc_text,
ARRAY('Merchant_Name','PO_Number','Invoice_Date','Purchase_Order_Date','Total')) as extracted
FROM classified_data
WHERE doc_classification = 'Purchase_Order'

-- COMMAND ----------

SELECT *
FROM purchase_order_data

-- COMMAND ----------

CREATE OR REPLACE TABLE idp.finance.purchase_orders AS
SELECT path,
extracted.Merchant_Name AS Merchant,
extracted.PO_Number AS Purchase_Order_Number,
extracted.Purchase_Order_Date AS Purchase_Order_Date,
extracted.Total AS Total
FROM purchase_order_data

-- COMMAND ----------

SELECT *
FROM idp.finance.purchase_orders

-- COMMAND ----------

CREATE OR REPLACE TABLE receipt_data AS
SELECT *,
ai_extract(doc_text,
ARRAY('Merchant_Name','Receipt_Number','Transaction_Date','Total', 'Payment_Method')) as extracted
FROM classified_data
WHERE doc_classification = 'Receipt'

-- COMMAND ----------

SELECT * 
FROM receipt_data

-- COMMAND ----------

CREATE OR REPLACE TABLE idp.finance.receipts AS
SELECT path,
extracted.Merchant_Name AS Merchant,
extracted.Receipt_Number AS Receipt_Number,
extracted.Transaction_Date AS Transaction_Date,
extracted.Total AS Total,
extracted.Payment_Method AS Payment_Method
FROM receipt_data

-- COMMAND ----------

SELECT *
FROM idp.finance.receipts