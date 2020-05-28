CREATE TABLE [dbo].[p_mms_membership_snapshot] (
    [p_mms_membership_snapshot_id]         BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [membership_id]                        INT          NULL,
    [s_mms_membership_snapshot_1_id]       BIGINT       NULL,
    [l_mms_membership_snapshot_1_id]       BIGINT       NULL,
    [l_mms_membership_snapshot_id]         BIGINT       NULL,
    [s_mms_membership_snapshot_id]         BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

