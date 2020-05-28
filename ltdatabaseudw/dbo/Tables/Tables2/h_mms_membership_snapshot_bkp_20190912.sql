CREATE TABLE [dbo].[h_mms_membership_snapshot_bkp_20190912] (
    [h_mms_membership_snapshot_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [membership_id]                INT          NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_deleted]                   BIT          NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

