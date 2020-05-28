CREATE TABLE [dbo].[d_mms_membership_audit] (
    [d_mms_membership_audit_id]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [fact_mms_membership_audit_key] CHAR (32)      NULL,
    [membership_audit_id]           INT            NULL,
    [modified_dim_date_key]         CHAR (32)      NULL,
    [modified_dim_employee_key]     CHAR (32)      NULL,
    [modified_dim_time_key]         CHAR (32)      NULL,
    [new_value]                     VARCHAR (1000) NULL,
    [old_value]                     VARCHAR (1000) NULL,
    [source_column_name]            VARCHAR (50)   NULL,
    [source_row_key]                CHAR (32)      NULL,
    [update_flag]                   CHAR (1)       NULL,
    [p_mms_membership_audit_id]     BIGINT         NOT NULL,
    [dv_load_date_time]             DATETIME       NULL,
    [dv_load_end_date_time]         DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_membership_audit]([dv_batch_id] ASC);

