CREATE TABLE [dbo].[d_mms_membership_modification_flex_reason_history] (
    [d_mms_membership_modification_flex_reason_history_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                              CHAR (32)    NOT NULL,
    [membership_modification_flex_reason_id]               INT          NULL,
    [inserted_date_time]                                   DATETIME     NULL,
    [membership_modification_request_id]                   INT          NULL,
    [updated_date_time]                                    DATETIME     NULL,
    [val_flex_reason_id]                                   INT          NULL,
    [p_mms_membership_modification_flex_reason_id]         BIGINT       NOT NULL,
    [deleted_flag]                                         INT          NULL,
    [dv_load_date_time]                                    DATETIME     NULL,
    [dv_load_end_date_time]                                DATETIME     NULL,
    [dv_batch_id]                                          BIGINT       NOT NULL,
    [dv_inserted_date_time]                                DATETIME     NOT NULL,
    [dv_insert_user]                                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                                 DATETIME     NULL,
    [dv_update_user]                                       VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

