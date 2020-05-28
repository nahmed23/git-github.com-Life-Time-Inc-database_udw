CREATE TABLE [dbo].[stage_hash_mms_Drawer] (
    [stage_hash_mms_Drawer_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [DrawerID]                 INT             NULL,
    [ClubID]                   INT             NULL,
    [LockedFlag]               BIT             NULL,
    [Description]              VARCHAR (50)    NULL,
    [InsertedDateTime]         DATETIME        NULL,
    [UpdatedDateTime]          DATETIME        NULL,
    [StartingCashAmount]       DECIMAL (26, 6) NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

