CREATE TABLE [dbo].[d_mms_membership_attribute] (
    [d_mms_membership_attribute_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)    NOT NULL,
    [dim_mms_membership_attribute_key] CHAR (32)    NULL,
    [membership_attribute_id]          INT          NULL,
    [dim_mms_membership_key]           VARCHAR (32) NULL,
    [effective_from_date_time]         DATETIME     NULL,
    [effective_thru_date_time]         DATETIME     NULL,
    [membership_attribute_value]       VARCHAR (50) NULL,
    [val_membership_attribute_type_id] INT          NULL,
    [p_mms_membership_attribute_id]    BIGINT       NOT NULL,
    [deleted_flag]                     INT          NULL,
    [dv_load_date_time]                DATETIME     NULL,
    [dv_load_end_date_time]            DATETIME     NULL,
    [dv_batch_id]                      BIGINT       NOT NULL,
    [dv_inserted_date_time]            DATETIME     NOT NULL,
    [dv_insert_user]                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]             DATETIME     NULL,
    [dv_update_user]                   VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

