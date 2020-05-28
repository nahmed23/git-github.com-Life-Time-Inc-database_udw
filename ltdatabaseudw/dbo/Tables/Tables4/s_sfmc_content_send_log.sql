CREATE TABLE [dbo].[s_sfmc_content_send_log] (
    [s_sfmc_content_send_log_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [member_id]                  VARCHAR (4000) NULL,
    [content_guid]               VARCHAR (4000) NULL,
    [is_test_send]               VARCHAR (4000) NULL,
    [subscriber_key]             VARCHAR (4000) NULL,
    [insert_date_time]           DATETIME       NULL,
    [dv_load_date_time]          DATETIME       NOT NULL,
    [dv_r_load_source_id]        BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL,
    [dv_hash]                    CHAR (32)      NOT NULL,
    [dv_deleted]                 BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

