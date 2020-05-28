CREATE TABLE [dbo].[MMSDimExchangeRate] (
    [DimExchangeRateKey]           INT             NOT NULL,
    [EffectiveDimDateKey]          INT             NOT NULL,
    [FromCurrencyCode]             VARCHAR (15)    NOT NULL,
    [ToCurrencyCode]               VARCHAR (15)    NOT NULL,
    [ExchangeRateTypeDescription]  VARCHAR (50)    NOT NULL,
    [ExchangeRate]                 DECIMAL (14, 4) NOT NULL,
    [SourceDailyAverageDimDateKey] INT             NOT NULL,
    [InsertedDateTime]             DATETIME2 (7)   NOT NULL,
    [InsertUser]                   VARCHAR (50)    NOT NULL,
    [UpdatedDateTime]              DATETIME2 (7)   NULL,
    [UpdateUser]                   VARCHAR (50)    NULL,
    [BatchID]                      INT             NOT NULL,
    [NKEffectiveDate]              DATETIME2 (7)   NULL,
    [NKDailyAverageDate]           DATETIME2 (7)   NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

