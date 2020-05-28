CREATE TABLE [dbo].[s_mms_membership_audit] (
    [s_mms_membership_audit_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [membership_audit_id]       INT            NULL,
    [operation]                 VARCHAR (10)   NULL,
    [modified_date_time]        DATETIME       NULL,
    [modified_user]             NVARCHAR (50)  NULL,
    [column_name]               VARCHAR (50)   NULL,
    [old_value]                 VARCHAR (1000) NULL,
    [new_value]                 VARCHAR (1000) NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_membership_audit]
    ON [dbo].[s_mms_membership_audit]([bk_hash] ASC, [s_mms_membership_audit_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership_audit]([dv_batch_id] ASC);

