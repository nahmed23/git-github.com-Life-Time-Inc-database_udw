CREATE TABLE [dbo].[stage_hash_spabiz_LOG] (
    [stage_hash_spabiz_LOG_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [APID]                     DECIMAL (26, 6) NULL,
    [APDATAID]                 DECIMAL (26, 6) NULL,
    [ID]                       DECIMAL (26, 6) NULL,
    [TIMEID]                   DECIMAL (26, 6) NULL,
    [ACTION]                   DECIMAL (26, 6) NULL,
    [BYSTAFFID]                DECIMAL (26, 6) NULL,
    [TIMESTAMP]                DATETIME        NULL,
    [CUSTID]                   DECIMAL (26, 6) NULL,
    [STAFFID]                  DECIMAL (26, 6) NULL,
    [SERVICEID]                DECIMAL (26, 6) NULL,
    [STARTTIME]                DATETIME        NULL,
    [ENDTIME]                  DATETIME        NULL,
    [STORE_NUMBER]             DECIMAL (26, 6) NULL,
    [COUNTERID]                DECIMAL (26, 6) NULL,
    [STOREID]                  DECIMAL (26, 6) NULL,
    [EDITTIME]                 DATETIME        NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

