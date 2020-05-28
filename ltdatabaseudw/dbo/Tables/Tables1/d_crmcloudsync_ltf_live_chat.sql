CREATE TABLE [dbo].[d_crmcloudsync_ltf_live_chat] (
    [d_crmcloudsync_ltf_live_chat_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [activity_id]                     VARCHAR (36)    NULL,
    [actual_start_dim_date_key]       CHAR (8)        NULL,
    [description]                     VARCHAR (8000)  NULL,
    [dim_club_key]                    CHAR (32)       NULL,
    [ltf_club_name]                   NVARCHAR (100)  NULL,
    [ltf_email_address_1]             NVARCHAR (100)  NULL,
    [ltf_first_name]                  NVARCHAR (50)   NULL,
    [ltf_last_name]                   NVARCHAR (50)   NULL,
    [ltf_line_of_business]            INT             NULL,
    [ltf_line_of_business_name]       NVARCHAR (255)  NULL,
    [ltf_referring_url]               NVARCHAR (4000) NULL,
    [ltf_transcript]                  VARCHAR (8000)  NULL,
    [subject]                         NVARCHAR (200)  NULL,
    [p_crmcloudsync_ltf_live_chat_id] BIGINT          NOT NULL,
    [deleted_flag]                    INT             NULL,
    [dv_load_date_time]               DATETIME        NULL,
    [dv_load_end_date_time]           DATETIME        NULL,
    [dv_batch_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

