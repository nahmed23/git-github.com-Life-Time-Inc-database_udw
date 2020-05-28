CREATE TABLE [dbo].[d_mms_child_center_usage_exception] (
    [d_mms_child_center_usage_exception_id]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)      NOT NULL,
    [fact_mms_child_center_usage_exception_key] CHAR (32)      NULL,
    [child_center_usage_exception_id]           INT            NULL,
    [dim_employee_key]                          CHAR (32)      NULL,
    [exception_dim_description_key]             NVARCHAR (100) NULL,
    [fact_mms_child_center_usage_key]           CHAR (32)      NULL,
    [p_mms_child_center_usage_exception_id]     BIGINT         NOT NULL,
    [dv_load_date_time]                         DATETIME       NULL,
    [dv_load_end_date_time]                     DATETIME       NULL,
    [dv_batch_id]                               BIGINT         NOT NULL,
    [dv_inserted_date_time]                     DATETIME       NOT NULL,
    [dv_insert_user]                            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                      DATETIME       NULL,
    [dv_update_user]                            VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_child_center_usage_exception]([dv_batch_id] ASC);

