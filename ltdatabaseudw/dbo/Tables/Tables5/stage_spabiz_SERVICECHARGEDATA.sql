CREATE TABLE [dbo].[stage_spabiz_SERVICECHARGEDATA] (
    [stage_spabiz_SERVICECHARGEDATA_id] BIGINT          NOT NULL,
    [ID]                                DECIMAL (26, 6) NULL,
    [COUNTERID]                         DECIMAL (26, 6) NULL,
    [STOREID]                           DECIMAL (26, 6) NULL,
    [EDITTIME]                          DATETIME        NULL,
    [SERVICECHARGEID]                   DECIMAL (26, 6) NULL,
    [SERVICEID]                         DECIMAL (26, 6) NULL,
    [DEPTCAT]                           DECIMAL (26, 6) NULL,
    [STORE_NUMBER]                      DECIMAL (26, 6) NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

