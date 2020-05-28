CREATE TABLE [dbo].[s_mms_member_attribute] (
    [s_mms_member_attribute_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [member_attribute_id]       INT          NULL,
    [attribute_value]           VARCHAR (50) NULL,
    [expiration_date]           DATETIME     NULL,
    [inserted_date_time]        DATETIME     NULL,
    [updated_date_time]         DATETIME     NULL,
    [dv_load_date_time]         DATETIME     NOT NULL,
    [dv_r_load_source_id]       BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL,
    [dv_hash]                   CHAR (32)    NOT NULL,
    [dv_deleted]                BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

