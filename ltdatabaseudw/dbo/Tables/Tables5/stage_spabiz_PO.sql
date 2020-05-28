CREATE TABLE [dbo].[stage_spabiz_PO] (
    [stage_spabiz_PO_id] BIGINT          NOT NULL,
    [ID]                 DECIMAL (26, 6) NULL,
    [COUNTERID]          DECIMAL (26, 6) NULL,
    [STOREID]            DECIMAL (26, 6) NULL,
    [EDITTIME]           DATETIME        NULL,
    [NUM]                VARCHAR (150)   NULL,
    [VENDORID]           DECIMAL (26, 6) NULL,
    [Date]               DATETIME        NULL,
    [STAFFID]            DECIMAL (26, 6) NULL,
    [STATUS]             DECIMAL (26, 6) NULL,
    [PAYMENT]            VARCHAR (150)   NULL,
    [DISCOUNT]           DECIMAL (26, 6) NULL,
    [TAX]                DECIMAL (26, 6) NULL,
    [TOTAL]              DECIMAL (26, 6) NULL,
    [SORTBY]             DECIMAL (26, 6) NULL,
    [RETAILTOTAL]        DECIMAL (26, 6) NULL,
    [SUBTOTAL]           DECIMAL (26, 6) NULL,
    [DELETEDATE]         DATETIME        NULL,
    [STORE_NUMBER]       DECIMAL (26, 6) NULL,
    [dv_batch_id]        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

