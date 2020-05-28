CREATE TABLE [dbo].[s_mms_child_center_usage] (
    [s_mms_child_center_usage_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [child_center_usage_id]       INT          NULL,
    [check_in_date_time]          DATETIME     NULL,
    [check_out_date_time]         DATETIME     NULL,
    [utc_check_in_date_time]      DATETIME     NULL,
    [check_in_date_time_zone]     VARCHAR (4)  NULL,
    [utc_check_out_date_time]     DATETIME     NULL,
    [check_out_date_time_zone]    VARCHAR (4)  NULL,
    [inserted_date_time]          DATETIME     NULL,
    [updated_date_time]           DATETIME     NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_batch_id]                 BIGINT       NOT NULL,
    [dv_r_load_source_id]         BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_hash]                     CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_child_center_usage]
    ON [dbo].[s_mms_child_center_usage]([bk_hash] ASC, [s_mms_child_center_usage_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_child_center_usage]([dv_batch_id] ASC);

