CREATE TABLE [dbo].[stage_hash_spabiz_INVADJ] (
    [stage_hash_spabiz_INVADJ_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [ID]                          DECIMAL (26, 6) NULL,
    [COUNTERID]                   DECIMAL (26, 6) NULL,
    [STOREID]                     DECIMAL (26, 6) NULL,
    [EDITTIME]                    DATETIME        NULL,
    [Date]                        DATETIME        NULL,
    [STAFFID]                     DECIMAL (26, 6) NULL,
    [STATUS]                      DECIMAL (26, 6) NULL,
    [NUM]                         VARCHAR (150)   NULL,
    [TOTAL]                       DECIMAL (26, 6) NULL,
    [STORE_NUMBER]                DECIMAL (26, 6) NULL,
    [GLACCOUNT]                   VARCHAR (60)    NULL,
    [TRANSFERLOC]                 DECIMAL (26, 6) NULL,
    [TRANSFERROID]                DECIMAL (26, 6) NULL,
    [TRANSFERVENDORID]            DECIMAL (26, 6) NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

