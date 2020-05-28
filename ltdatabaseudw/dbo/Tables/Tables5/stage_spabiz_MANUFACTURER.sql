CREATE TABLE [dbo].[stage_spabiz_MANUFACTURER] (
    [stage_spabiz_MANUFACTURER_id] BIGINT          NOT NULL,
    [ID]                           DECIMAL (26, 6) NULL,
    [COUNTERID]                    DECIMAL (26, 6) NULL,
    [STOREID]                      DECIMAL (26, 6) NULL,
    [EDITTIME]                     DATETIME        NULL,
    [Delete]                       DECIMAL (26, 6) NULL,
    [DELETEDATE]                   DATETIME        NULL,
    [NAME]                         VARCHAR (150)   NULL,
    [QUICKID]                      VARCHAR (150)   NULL,
    [REFRESH]                      DECIMAL (26, 6) NULL,
    [STORE_NUMBER]                 DECIMAL (26, 6) NULL,
    [NEWID]                        DECIMAL (26, 6) NULL,
    [MANUFACTURERBACKUPID]         DECIMAL (26, 6) NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

