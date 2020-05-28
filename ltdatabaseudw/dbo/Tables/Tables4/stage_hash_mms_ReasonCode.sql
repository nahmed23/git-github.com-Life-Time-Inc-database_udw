CREATE TABLE [dbo].[stage_hash_mms_ReasonCode] (
    [stage_hash_mms_ReasonCode_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [ReasonCodeID]                 INT          NULL,
    [Name]                         VARCHAR (15) NULL,
    [Description]                  VARCHAR (50) NULL,
    [SortOrder]                    INT          NULL,
    [DisplayUIFlag]                BIT          NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

