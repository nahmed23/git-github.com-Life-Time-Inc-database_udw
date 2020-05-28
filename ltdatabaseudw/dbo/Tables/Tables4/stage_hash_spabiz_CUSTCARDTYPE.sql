CREATE TABLE [dbo].[stage_hash_spabiz_CUSTCARDTYPE] (
    [stage_hash_spabiz_CUSTCARDTYPE_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)       NOT NULL,
    [ID]                                DECIMAL (26, 6) NULL,
    [COUNTERID]                         DECIMAL (26, 6) NULL,
    [STOREID]                           DECIMAL (26, 6) NULL,
    [EDITTIME]                          DATETIME        NULL,
    [Delete]                            DECIMAL (26, 6) NULL,
    [DELETEDATE]                        DATETIME        NULL,
    [NAME]                              VARCHAR (150)   NULL,
    [RETAILPRICE]                       DECIMAL (26, 6) NULL,
    [DAYSGOODFOR]                       DECIMAL (26, 6) NULL,
    [SERIALNUMCOUNTER]                  DECIMAL (26, 6) NULL,
    [STORE_NUMBER]                      DECIMAL (26, 6) NULL,
    [PAYMENTINTERVAL]                   DECIMAL (26, 6) NULL,
    [SERVICEDISC]                       VARCHAR (30)    NULL,
    [PRODDISC]                          VARCHAR (30)    NULL,
    [DISCOUNTID]                        DECIMAL (26, 6) NULL,
    [DISPCOLOR]                         VARCHAR (60)    NULL,
    [INITIALPRICE]                      DECIMAL (26, 6) NULL,
    [dv_load_date_time]                 DATETIME        NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

