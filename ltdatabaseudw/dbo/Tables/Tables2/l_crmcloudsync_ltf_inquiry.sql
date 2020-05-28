CREATE TABLE [dbo].[l_crmcloudsync_ltf_inquiry] (
    [l_crmcloudsync_ltf_inquiry_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [activity_id]                   VARCHAR (36)   NULL,
    [created_by]                    VARCHAR (36)   NULL,
    [created_on_behalf_by]          VARCHAR (36)   NULL,
    [ltf_cid]                       NVARCHAR (100) NULL,
    [ltf_gc_id]                     NVARCHAR (100) NULL,
    [ltf_gcl_id]                    NVARCHAR (100) NULL,
    [ltf_member_id]                 NVARCHAR (25)  NULL,
    [ltf_mms_clubid]                NVARCHAR (10)  NULL,
    [ltf_referring_contact_id]      VARCHAR (36)   NULL,
    [ltf_referring_member_id]       NVARCHAR (10)  NULL,
    [modified_by]                   VARCHAR (36)   NULL,
    [modified_on_behalf_by]         VARCHAR (36)   NULL,
    [owner_id]                      VARCHAR (36)   NULL,
    [owning_business_unit]          VARCHAR (36)   NULL,
    [owning_team]                   VARCHAR (36)   NULL,
    [owning_user]                   VARCHAR (36)   NULL,
    [process_id]                    VARCHAR (36)   NULL,
    [regarding_object_id]           VARCHAR (36)   NULL,
    [sender_mail_box_id]            VARCHAR (36)   NULL,
    [series_id]                     VARCHAR (36)   NULL,
    [service_id]                    VARCHAR (36)   NULL,
    [stage_id]                      VARCHAR (36)   NULL,
    [transaction_currency_id]       VARCHAR (36)   NULL,
    [ltf_visitor_id]                NVARCHAR (40)  NULL,
    [ltf_club_name]                 VARCHAR (36)   NULL,
    [ltf_user_id_lead_id]           NVARCHAR (100) NULL,
    [dv_load_date_time]             DATETIME       NOT NULL,
    [dv_r_load_source_id]           BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL,
    [dv_hash]                       CHAR (32)      NOT NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_ltf_inquiry]
    ON [dbo].[l_crmcloudsync_ltf_inquiry]([bk_hash] ASC, [l_crmcloudsync_ltf_inquiry_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_inquiry]([dv_batch_id] ASC);

