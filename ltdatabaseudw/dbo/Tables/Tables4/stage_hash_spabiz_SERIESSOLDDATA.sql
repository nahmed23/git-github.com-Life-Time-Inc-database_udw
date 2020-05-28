CREATE TABLE [dbo].[stage_hash_spabiz_SERIESSOLDDATA] (
    [stage_hash_spabiz_SERIESSOLDDATA_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [ID]                                  DECIMAL (26, 6) NULL,
    [COUNTERID]                           DECIMAL (26, 6) NULL,
    [STOREID]                             DECIMAL (26, 6) NULL,
    [EDITTIME]                            DATETIME        NULL,
    [SERIESID]                            DECIMAL (26, 6) NULL,
    [SERIESSOLDID]                        DECIMAL (26, 6) NULL,
    [SERVICEID]                           DECIMAL (26, 6) NULL,
    [SERVICEPRICE]                        DECIMAL (26, 6) NULL,
    [PRICETYPE]                           DECIMAL (26, 6) NULL,
    [ORDERINDEX]                          VARCHAR (150)   NULL,
    [TICKETID]                            DECIMAL (26, 6) NULL,
    [CUSTID]                              DECIMAL (26, 6) NULL,
    [Date]                                DATETIME        NULL,
    [STORE_NUMBER]                        DECIMAL (26, 6) NULL,
    [SERVICECHARGEAMT]                    DECIMAL (26, 6) NULL,
    [TIPAMT]                              DECIMAL (26, 6) NULL,
    [dv_load_date_time]                   DATETIME        NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

