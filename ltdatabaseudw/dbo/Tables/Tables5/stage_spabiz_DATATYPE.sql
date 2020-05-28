CREATE TABLE [dbo].[stage_spabiz_DATATYPE] (
    [stage_spabiz_DATATYPE_id] BIGINT          NOT NULL,
    [ID]                       DECIMAL (26, 6) NULL,
    [STORE_NUMBER]             DECIMAL (26, 6) NULL,
    [CAPTION]                  VARCHAR (150)   NULL,
    [TYPE]                     DECIMAL (26, 6) NULL,
    [COUNTERID]                DECIMAL (26, 6) NULL,
    [STOREID]                  DECIMAL (26, 6) NULL,
    [EDITTIME]                 DATETIME        NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

