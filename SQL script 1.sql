CREATE SCHEMA synapse;

========================

CREATE MASTER KEY ENCRYPTION BY PASSWORD ='12345@test'

========================
CREATE DATABASE SCOPED CREDENTIAL AzureStorageAccountKey
WITH IDENTITY = 'fraudana',
SECRET ='ALo5uRN/KlhUUsqQKFZkuyVCGg9Py2DvtNz9rcKeNbiTORg1+UQN/kblQS6l1CanW4/Q6whnmFC6+AStsiu/Fw=='

=========================
CREATE EXTERNAL DATA SOURCE CSVDataSource WITH
(
    TYPE= HADOOP,
    LOCATION='wasbs://file@fraudana.blob.core.windows.net',
    CREDENTIAL = AzureStorageAccountKey

);
========================


CREATE EXTERNAL FILE FORMAT CSVFileFormat
WITH (
    FORMAT_TYPE= DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR=',',
        STRING_DELIMITER='"',
        FIRST_ROW=2,
        USE_TYPE_DEFAULT=TRUE
    )
);
GO

CREATE EXTERNAL FILE FORMAT csv
WITH (
    FORMAT_TYPE= DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR=',',
        STRING_DELIMITER='',
        DATE_FORMAT='',
        USE_TYPE_DEFAULT=FALSE
    )
);
GO

===========================

 CREATE EXTERNAL TABLE synapse.exCreditCard
(    [amt] float,
    [is_fraud] INT,
    [yob] BIGINT,
    [trans_year] BIGINT,
    [trans_hour] BIGINT,
    [cat_id] BIGINT,
	[city_id] BIGINT
)
WITH(
    LOCATION='fraud_analytic.csv',
    DATA_SOURCE=[CSVDataSource],
    FILE_FORMAT=[CSVFileFormat]
);
GO

========================
CREATE EXTERNAL TABLE synapse.[MLModelExt]
(
    [Model][VARBINARY](max) NULL
)
WITH
(
    LOCATION='credit_card_fraud.onnx',
    DATA_SOURCE=[CSVDataSource],
    FILE_FORMAT=[csv],
    REJECT_TYPE= VALUE,
    REJECT_VALUE=0
);
GO

==========================
DECLARE @modelexample VARBINARY(max)=(SELECT[Model] FROM synapse.[MLModelExt]);

SELECT
d.*, p.*
INTO synapse.Trans
FROM PREDICT(MODEL= @modelexample,
DATA= synapse.exCreditCard AS d,
RUNNTIME = ONNX) WITH (prediction bigint) AS p;

==========================
CREATE VIEW dbo.CREDITCARDLOC AS
SELECT
    credit.amt,
    cities.lat,
    cities.long,
    credit.is_fraud,
    credit.yob,
    credit.trans_hour,
    credit.cat_id,
    credit.trans_year,
    credit.city_id,
    credit.prediction
FROM 
OPENROWSET(
    BULK 'https://fraudana.blob.core.windows.net/file/CreditCardOutput.csv',
    FORMAT ='CSV',
    FIELDTERMINATOR=',',
    FIRSTROW=2,
    ESCAPECHAR='\\'
)
WITH (
    [amt] float,
    [is_fraud] INT,
    [yob] INT,
    [trans_year] BIGINT,
    [trans_hour] BIGINT,
    [cat_id] BIGINT,
    [city_id] BIGINT,
    [prediction] INT) AS [credit]
LEFT JOIN
OPENROWSET(
    BULK 'https://fraudana.blob.core.windows.net/file/uscity.csv',
    FORMAT ='CSV',
    FIELDTERMINATOR=',',
    FIRSTROW=2,
    ESCAPECHAR='\\'
)
WITH (
    [city] VARCHAR(100) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
    [city_id] BIGINT,
    [lat] float,
    [long] float) AS [cities] ON credit.city_id=cities.city_id
LEFT JOIN
OPENROWSET(
    BULK 'https://fraudana.blob.core.windows.net/file/category.csv',
    FORMAT ='CSV',
    FIELDTERMINATOR=',',
    FIRSTROW=2,
    ESCAPECHAR='\\'
)
WITH (
    [category] VARCHAR(100) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
    [cat_id] BIGINT)AS [category]
    ON credit.cat_id=category.cat_id
