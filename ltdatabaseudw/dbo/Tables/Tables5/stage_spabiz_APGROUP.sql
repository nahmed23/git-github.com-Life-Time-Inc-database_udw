CREATE TABLE [dbo].[stage_spabiz_APGROUP] (
    [stage_spabiz_APGROUP_id] BIGINT          NOT NULL,
    [ID]                      DECIMAL (26, 6) NULL,
    [STORE_NUMBER]            DECIMAL (26, 6) NULL,
    [COUNTERID]               DECIMAL (26, 6) NULL,
    [STOREID]                 DECIMAL (26, 6) NULL,
    [EDITTIME]                DATETIME        NULL,
    [Delete]                  DECIMAL (26, 6) NULL,
    [DELETEDATE]              DATETIME        NULL,
    [NAME]                    VARCHAR (150)   NULL,
    [COLS]                    DECIMAL (26, 6) NULL,
    [QUICKKEY]                VARCHAR (150)   NULL,
    [TAB]                     DECIMAL (26, 6) NULL,
    [TABORDER]                DECIMAL (26, 6) NULL,
    [dv_batch_id]             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

