CREATE TABLE [dbo].[stage_spabiz_GIFT] (
    [stage_spabiz_GIFT_id] BIGINT          NOT NULL,
    [ID]                   DECIMAL (26, 6) NULL,
    [COUNTERID]            DECIMAL (26, 6) NULL,
    [STOREID]              DECIMAL (26, 6) NULL,
    [EDITTIME]             DATETIME        NULL,
    [Delete]               DECIMAL (26, 6) NULL,
    [DELETEDATE]           DATETIME        NULL,
    [NAME]                 VARCHAR (150)   NULL,
    [PAYCOMMISSION]        DECIMAL (26, 6) NULL,
    [RETAILPRICE]          DECIMAL (26, 6) NULL,
    [PRICECHANGABLE]       DECIMAL (26, 6) NULL,
    [DAYSGOODFOR]          DECIMAL (26, 6) NULL,
    [USEFOR]               DECIMAL (26, 6) NULL,
    [REFUNDABLE]           DECIMAL (26, 6) NULL,
    [STORE_NUMBER]         DECIMAL (26, 6) NULL,
    [dv_batch_id]          BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

