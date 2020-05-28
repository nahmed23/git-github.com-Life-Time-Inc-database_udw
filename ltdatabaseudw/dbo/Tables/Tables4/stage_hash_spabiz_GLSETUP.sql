CREATE TABLE [dbo].[stage_hash_spabiz_GLSETUP] (
    [stage_hash_spabiz_GLSETUP_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [STORE_NUMBER]                 DECIMAL (26, 6) NULL,
    [DESCRIPTION]                  VARCHAR (150)   NULL,
    [GLACCOUNT]                    VARCHAR (60)    NULL,
    [EDITTIME]                     DATETIME        NULL,
    [STATUS]                       DECIMAL (26, 6) NULL,
    [DELETED]                      DECIMAL (26, 6) NULL,
    [EXPENSE]                      DECIMAL (26, 6) NULL,
    [OPTIONAL]                     DECIMAL (26, 6) NULL,
    [RANK]                         DECIMAL (26, 6) NULL,
    [ID]                           DECIMAL (26, 6) NULL,
    [COUNTERID]                    DECIMAL (26, 6) NULL,
    [STOREID]                      DECIMAL (26, 6) NULL,
    [dv_load_date_time]            DATETIME        NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

