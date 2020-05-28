CREATE TABLE [dbo].[stage_spabiz_CATEGORY] (
    [stage_spabiz_CATEGORY_id] BIGINT          NOT NULL,
    [ID]                       DECIMAL (26, 6) NULL,
    [STORE_NUMBER]             DECIMAL (26, 6) NULL,
    [COUNTERID]                DECIMAL (26, 6) NULL,
    [STOREID]                  DECIMAL (26, 6) NULL,
    [EDITTIME]                 DATETIME        NULL,
    [Delete]                   DECIMAL (26, 6) NULL,
    [DELETEDATE]               DATETIME        NULL,
    [NAME]                     VARCHAR (150)   NULL,
    [QUICKID]                  VARCHAR (150)   NULL,
    [DATATYPE]                 DECIMAL (26, 6) NULL,
    [PARENTID]                 DECIMAL (26, 6) NULL,
    [FASTINDEX]                VARCHAR (30)    NULL,
    [COSMETIC]                 DECIMAL (26, 6) NULL,
    [DISPLAYCOLOR]             VARCHAR (150)   NULL,
    [GLACCOUNT]                VARCHAR (90)    NULL,
    [LVL]                      DECIMAL (26, 6) NULL,
    [Level]                    DECIMAL (26, 6) NULL,
    [WEBBOOK]                  DECIMAL (26, 6) NULL,
    [NEWID]                    DECIMAL (26, 6) NULL,
    [CATEGORYBACKUPID]         DECIMAL (26, 6) NULL,
    [WEBVIEW]                  DECIMAL (26, 6) NULL,
    [CLASS]                    VARCHAR (3)     NULL,
    [DEPARTMENT]               DECIMAL (26, 6) NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

