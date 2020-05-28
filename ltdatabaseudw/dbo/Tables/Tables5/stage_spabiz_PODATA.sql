CREATE TABLE [dbo].[stage_spabiz_PODATA] (
    [stage_spabiz_PODATA_id] BIGINT          NOT NULL,
    [ID]                     DECIMAL (26, 6) NULL,
    [COUNTERID]              DECIMAL (26, 6) NULL,
    [STOREID]                DECIMAL (26, 6) NULL,
    [EDITTIME]               DATETIME        NULL,
    [Date]                   DATETIME        NULL,
    [POID]                   DECIMAL (26, 6) NULL,
    [TYPE]                   DECIMAL (26, 6) NULL,
    [VENDORID]               DECIMAL (26, 6) NULL,
    [LINENUM]                DECIMAL (26, 6) NULL,
    [PRODUCTID]              DECIMAL (26, 6) NULL,
    [NORMALCOST]             DECIMAL (26, 6) NULL,
    [COST]                   DECIMAL (26, 6) NULL,
    [EXTCOST]                DECIMAL (26, 6) NULL,
    [QTYORD]                 DECIMAL (26, 6) NULL,
    [QTYREC]                 DECIMAL (26, 6) NULL,
    [STATUS]                 DECIMAL (26, 6) NULL,
    [CATID]                  DECIMAL (26, 6) NULL,
    [MARGIN]                 DECIMAL (26, 6) NULL,
    [RETAILPRICE]            DECIMAL (26, 6) NULL,
    [NAME]                   VARCHAR (150)   NULL,
    [STORE_NUMBER]           DECIMAL (26, 6) NULL,
    [dv_batch_id]            BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

