CREATE TABLE [dbo].[d_crmcloudsync_ltf_inquiry] (
    [d_crmcloudsync_ltf_inquiry_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [dim_crm_ltf_inquiry_key]       VARCHAR (32)   NULL,
    [activity_id]                   VARCHAR (36)   NULL,
    [contact_source]                NVARCHAR (100) NULL,
    [dim_club_key]                  VARCHAR (32)   NULL,
    [first_name]                    VARCHAR (50)   NULL,
    [last_name]                     VARCHAR (50)   NULL,
    [lead_source]                   NVARCHAR (100) NULL,
    [referring_member_flag]         CHAR (1)       NULL,
    [state_code_name]               VARCHAR (255)  NULL,
    [p_crmcloudsync_ltf_inquiry_id] BIGINT         NOT NULL,
    [deleted_flag]                  INT            NULL,
    [dv_load_date_time]             DATETIME       NULL,
    [dv_load_end_date_time]         DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

