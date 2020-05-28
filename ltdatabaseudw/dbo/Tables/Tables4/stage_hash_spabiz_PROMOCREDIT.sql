CREATE TABLE [dbo].[stage_hash_spabiz_PROMOCREDIT] (
    [stage_hash_spabiz_PROMOCREDIT_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [ID]                               DECIMAL (26, 6) NULL,
    [COUNTERID]                        DECIMAL (26, 6) NULL,
    [STOREID]                          DECIMAL (26, 6) NULL,
    [EDITTIME]                         DATETIME        NULL,
    [STAFFID]                          DECIMAL (26, 6) NULL,
    [STATUS]                           DECIMAL (26, 6) NULL,
    [CUSTID]                           DECIMAL (26, 6) NULL,
    [PROMOID]                          DECIMAL (26, 6) NULL,
    [SERIALNUM]                        VARCHAR (150)   NULL,
    [AMOUNT]                           DECIMAL (26, 6) NULL,
    [BALANCE]                          DECIMAL (26, 6) NULL,
    [Date]                             DATETIME        NULL,
    [DAYSGOOD]                         DECIMAL (26, 6) NULL,
    [LASTUSED]                         DATETIME        NULL,
    [ADDRESS1]                         VARCHAR (150)   NULL,
    [STORE_NUMBER]                     DECIMAL (26, 6) NULL,
    [dv_load_date_time]                DATETIME        NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

