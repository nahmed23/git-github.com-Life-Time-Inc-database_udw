CREATE TABLE [dbo].[stage_hash_spabiz_STAFFPRICING] (
    [stage_hash_spabiz_STAFFPRICING_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)       NOT NULL,
    [ID]                                DECIMAL (26, 6) NULL,
    [COUNTERID]                         DECIMAL (26, 6) NULL,
    [STOREID]                           DECIMAL (26, 6) NULL,
    [EDITTIME]                          DATETIME        NULL,
    [SERVICEID]                         DECIMAL (26, 6) NULL,
    [STAFFID]                           DECIMAL (26, 6) NULL,
    [STAFFSERVICEINDEX]                 VARCHAR (150)   NULL,
    [USEPRICESPECIAL]                   DECIMAL (26, 6) NULL,
    [RETAILPRICE]                       DECIMAL (26, 6) NULL,
    [COST]                              DECIMAL (26, 6) NULL,
    [USETIMESPECIAL]                    DECIMAL (26, 6) NULL,
    [TIME]                              VARCHAR (30)    NULL,
    [PROCESS]                           VARCHAR (30)    NULL,
    [FINISH]                            VARCHAR (30)    NULL,
    [NEWEXTRATIME]                      VARCHAR (30)    NULL,
    [SALES_SERVICETOTAL]                DECIMAL (26, 6) NULL,
    [STORE_NUMBER]                      DECIMAL (26, 6) NULL,
    [dv_load_date_time]                 DATETIME        NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

