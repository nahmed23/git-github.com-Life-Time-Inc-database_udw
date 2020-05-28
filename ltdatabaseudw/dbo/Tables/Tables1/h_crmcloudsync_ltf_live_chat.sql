CREATE TABLE [dbo].[h_crmcloudsync_ltf_live_chat] (
    [h_crmcloudsync_ltf_live_chat_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [activity_id]                     VARCHAR (36) NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_r_load_source_id]             BIGINT       NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_deleted]                      BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

