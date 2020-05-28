CREATE TABLE [dbo].[stage_spabiz_APGROUPDATA] (
    [stage_spabiz_APGROUPDATA_id] BIGINT          NOT NULL,
    [ID]                          DECIMAL (26, 6) NULL,
    [STORE_NUMBER]                DECIMAL (26, 6) NULL,
    [COUNTERID]                   DECIMAL (26, 6) NULL,
    [STOREID]                     DECIMAL (26, 6) NULL,
    [EDITTIME]                    DATETIME        NULL,
    [GROUPID]                     DECIMAL (26, 6) NULL,
    [STAFFID]                     DECIMAL (26, 6) NULL,
    [ORDERNUM]                    DECIMAL (26, 6) NULL,
    [Order]                       VARCHAR (150)   NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

