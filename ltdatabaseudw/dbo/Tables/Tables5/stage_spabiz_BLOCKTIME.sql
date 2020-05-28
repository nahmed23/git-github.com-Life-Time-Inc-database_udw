CREATE TABLE [dbo].[stage_spabiz_BLOCKTIME] (
    [stage_spabiz_BLOCKTIME_id] BIGINT          NOT NULL,
    [ID]                        DECIMAL (26, 6) NULL,
    [COUNTERID]                 DECIMAL (26, 6) NULL,
    [STOREID]                   DECIMAL (26, 6) NULL,
    [EDITTIME]                  DATETIME        NULL,
    [QUICKID]                   VARCHAR (150)   NULL,
    [Delete]                    DECIMAL (26, 6) NULL,
    [DELETEDATE]                DATETIME        NULL,
    [NAME]                      VARCHAR (150)   NULL,
    [STORE_NUMBER]              DECIMAL (26, 6) NULL,
    [REDUCESPRODUCTIVITY]       DECIMAL (26, 6) NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

