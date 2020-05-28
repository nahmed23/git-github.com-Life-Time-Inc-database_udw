CREATE TABLE [dbo].[stage_spabiz_LOG] (
    [stage_spabiz_LOG_id] BIGINT          NOT NULL,
    [APID]                DECIMAL (26, 6) NULL,
    [APDATAID]            DECIMAL (26, 6) NULL,
    [ID]                  DECIMAL (26, 6) NULL,
    [TIMEID]              DECIMAL (26, 6) NULL,
    [ACTION]              DECIMAL (26, 6) NULL,
    [BYSTAFFID]           DECIMAL (26, 6) NULL,
    [TIMESTAMP]           DATETIME        NULL,
    [CUSTID]              DECIMAL (26, 6) NULL,
    [STAFFID]             DECIMAL (26, 6) NULL,
    [SERVICEID]           DECIMAL (26, 6) NULL,
    [STARTTIME]           DATETIME        NULL,
    [ENDTIME]             DATETIME        NULL,
    [STORE_NUMBER]        DECIMAL (26, 6) NULL,
    [COUNTERID]           DECIMAL (26, 6) NULL,
    [STOREID]             DECIMAL (26, 6) NULL,
    [EDITTIME]            DATETIME        NULL,
    [dv_batch_id]         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

