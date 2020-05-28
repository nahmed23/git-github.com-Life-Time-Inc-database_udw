CREATE TABLE [dbo].[stage_hash_mms_DeletedData] (
    [stage_hash_mms_DeletedData_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)     NOT NULL,
    [DeletedDataID]                 INT           NULL,
    [TableName]                     VARCHAR (100) NULL,
    [PrimaryKeyID]                  INT           NULL,
    [DeletedDateTime]               DATETIME      NULL,
    [dv_load_date_time]             DATETIME      NOT NULL,
    [dv_inserted_date_time]         DATETIME      NOT NULL,
    [dv_insert_user]                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]          DATETIME      NULL,
    [dv_update_user]                VARCHAR (50)  NULL,
    [dv_batch_id]                   BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

