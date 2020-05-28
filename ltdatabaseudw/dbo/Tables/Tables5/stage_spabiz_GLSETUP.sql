CREATE TABLE [dbo].[stage_spabiz_GLSETUP] (
    [stage_spabiz_GLSETUP_id] BIGINT          NOT NULL,
    [STORE_NUMBER]            DECIMAL (26, 6) NULL,
    [DESCRIPTION]             VARCHAR (150)   NULL,
    [GLACCOUNT]               VARCHAR (60)    NULL,
    [EDITTIME]                DATETIME        NULL,
    [STATUS]                  DECIMAL (26, 6) NULL,
    [DELETED]                 DECIMAL (26, 6) NULL,
    [EXPENSE]                 DECIMAL (26, 6) NULL,
    [OPTIONAL]                DECIMAL (26, 6) NULL,
    [RANK]                    DECIMAL (26, 6) NULL,
    [ID]                      DECIMAL (26, 6) NULL,
    [COUNTERID]               DECIMAL (26, 6) NULL,
    [STOREID]                 DECIMAL (26, 6) NULL,
    [dv_batch_id]             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

