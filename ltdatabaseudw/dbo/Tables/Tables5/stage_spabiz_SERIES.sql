CREATE TABLE [dbo].[stage_spabiz_SERIES] (
    [stage_spabiz_SERIES_id] BIGINT          NOT NULL,
    [ID]                     DECIMAL (26, 6) NULL,
    [COUNTERID]              DECIMAL (26, 6) NULL,
    [STOREID]                DECIMAL (26, 6) NULL,
    [EDITTIME]               DATETIME        NULL,
    [Delete]                 DECIMAL (26, 6) NULL,
    [NAME]                   VARCHAR (150)   NULL,
    [QUICKID]                VARCHAR (150)   NULL,
    [RETAILPRICE]            DECIMAL (26, 6) NULL,
    [TAXABLE]                DECIMAL (26, 6) NULL,
    [DELETEDATE]             DATETIME        NULL,
    [Order]                  DECIMAL (26, 6) NULL,
    [ORDERINDEX]             VARCHAR (150)   NULL,
    [STORE_NUMBER]           DECIMAL (26, 6) NULL,
    [MASTERSERIESID]         DECIMAL (26, 6) NULL,
    [dv_batch_id]            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

