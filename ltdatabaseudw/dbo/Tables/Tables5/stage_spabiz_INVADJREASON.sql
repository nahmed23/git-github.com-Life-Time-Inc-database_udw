CREATE TABLE [dbo].[stage_spabiz_INVADJREASON] (
    [stage_spabiz_INVADJREASON_id] BIGINT          NOT NULL,
    [ID]                           DECIMAL (26, 6) NULL,
    [COUNTERID]                    DECIMAL (26, 6) NULL,
    [STOREID]                      DECIMAL (26, 6) NULL,
    [EDITTIME]                     DATETIME        NULL,
    [Delete]                       DECIMAL (26, 6) NULL,
    [DELETEDATE]                   DATETIME        NULL,
    [NAME]                         VARCHAR (150)   NULL,
    [RECEIPTPRINTER]               VARCHAR (150)   NULL,
    [STORE_NUMBER]                 DECIMAL (26, 6) NULL,
    [GLACCOUNT]                    VARCHAR (60)    NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

