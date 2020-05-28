CREATE TABLE [dbo].[stage_spabiz_PACKAGE] (
    [stage_spabiz_PACKAGE_id] BIGINT          NOT NULL,
    [ID]                      DECIMAL (26, 6) NULL,
    [COUNTERID]               DECIMAL (26, 6) NULL,
    [STOREID]                 DECIMAL (26, 6) NULL,
    [EDITTIME]                DATETIME        NULL,
    [Delete]                  DECIMAL (26, 6) NULL,
    [DELETEDATE]              DATETIME        NULL,
    [NAME]                    VARCHAR (150)   NULL,
    [QUICKID]                 VARCHAR (150)   NULL,
    [RETAILPRICE]             DECIMAL (26, 6) NULL,
    [DEPTCAT]                 DECIMAL (26, 6) NULL,
    [TYPE]                    DECIMAL (26, 6) NULL,
    [TAXABLE]                 DECIMAL (26, 6) NULL,
    [STORE_NUMBER]            DECIMAL (26, 6) NULL,
    [dv_batch_id]             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

