CREATE TABLE [dbo].[stage_hash_mms_MembershipAudit] (
    [stage_hash_mms_MembershipAudit_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [MembershipAuditId]                 INT            NULL,
    [RowId]                             INT            NULL,
    [Operation]                         VARCHAR (10)   NULL,
    [ModifiedDateTime]                  DATETIME       NULL,
    [ModifiedUser]                      NVARCHAR (50)  NULL,
    [ColumnName]                        VARCHAR (50)   NULL,
    [OldValue]                          VARCHAR (1000) NULL,
    [NewValue]                          VARCHAR (1000) NULL,
    [dv_load_date_time]                 DATETIME       NOT NULL,
    [dv_inserted_date_time]             DATETIME       NOT NULL,
    [dv_insert_user]                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]              DATETIME       NULL,
    [dv_update_user]                    VARCHAR (50)   NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

