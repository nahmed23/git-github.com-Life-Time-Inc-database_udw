CREATE TABLE [dbo].[l_mms_package] (
    [l_mms_package_id]      BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [package_id]            INT          NULL,
    [member_id]             INT          NULL,
    [membership_id]         INT          NULL,
    [club_id]               INT          NULL,
    [employee_id]           INT          NULL,
    [val_package_status_id] SMALLINT     NULL,
    [mms_tran_id]           INT          NULL,
    [product_id]            INT          NULL,
    [tran_item_id]          INT          NULL,
    [external_package_id]   VARCHAR (50) NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL,
    [dv_deleted]            BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_package]([dv_batch_id] ASC);

