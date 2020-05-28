CREATE TABLE [dbo].[stage_spabiz_SERIESSOLDDATA] (
    [stage_spabiz_SERIESSOLDDATA_id] BIGINT          NOT NULL,
    [ID]                             DECIMAL (26, 6) NULL,
    [COUNTERID]                      DECIMAL (26, 6) NULL,
    [STOREID]                        DECIMAL (26, 6) NULL,
    [EDITTIME]                       DATETIME        NULL,
    [SERIESID]                       DECIMAL (26, 6) NULL,
    [SERIESSOLDID]                   DECIMAL (26, 6) NULL,
    [SERVICEID]                      DECIMAL (26, 6) NULL,
    [SERVICEPRICE]                   DECIMAL (26, 6) NULL,
    [PRICETYPE]                      DECIMAL (26, 6) NULL,
    [ORDERINDEX]                     VARCHAR (150)   NULL,
    [TICKETID]                       DECIMAL (26, 6) NULL,
    [CUSTID]                         DECIMAL (26, 6) NULL,
    [Date]                           DATETIME        NULL,
    [STORE_NUMBER]                   DECIMAL (26, 6) NULL,
    [SERVICECHARGEAMT]               DECIMAL (26, 6) NULL,
    [TIPAMT]                         DECIMAL (26, 6) NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

