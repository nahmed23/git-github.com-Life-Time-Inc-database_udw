CREATE TABLE [dbo].[stage_spabiz_INVADJDATA] (
    [stage_spabiz_INVADJDATA_id] BIGINT          NOT NULL,
    [ID]                         DECIMAL (26, 6) NULL,
    [COUNTERID]                  DECIMAL (26, 6) NULL,
    [STOREID]                    DECIMAL (26, 6) NULL,
    [EDITTIME]                   DATETIME        NULL,
    [Date]                       DATETIME        NULL,
    [ADJID]                      DECIMAL (26, 6) NULL,
    [QTY]                        DECIMAL (26, 6) NULL,
    [PRODUCTID]                  DECIMAL (26, 6) NULL,
    [STAFFID]                    DECIMAL (26, 6) NULL,
    [REASONID]                   DECIMAL (26, 6) NULL,
    [COST]                       DECIMAL (26, 6) NULL,
    [LAYERID]                    DECIMAL (26, 6) NULL,
    [SOURCETYPE]                 DECIMAL (26, 6) NULL,
    [SOURCEID]                   DECIMAL (26, 6) NULL,
    [STATUS]                     DECIMAL (26, 6) NULL,
    [CATID]                      DECIMAL (26, 6) NULL,
    [STORE_NUMBER]               DECIMAL (26, 6) NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

