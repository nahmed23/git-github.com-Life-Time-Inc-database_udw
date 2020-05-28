CREATE TABLE [dbo].[l_crmcloudsync_ltf_campaign_instance] (
    [l_crmcloudsync_ltf_campaign_instance_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)      NOT NULL,
    [created_by]                              VARCHAR (36)   NULL,
    [created_on_behalf_by]                    VARCHAR (36)   NULL,
    [ltf_campaign]                            VARCHAR (36)   NULL,
    [ltf_campaign_instance_id]                VARCHAR (36)   NULL,
    [ltf_club]                                VARCHAR (36)   NULL,
    [ltf_issued_by]                           VARCHAR (36)   NULL,
    [ltf_issuing_contact]                     VARCHAR (36)   NULL,
    [ltf_issuing_lead]                        VARCHAR (36)   NULL,
    [ltf_issuing_opportunity]                 VARCHAR (36)   NULL,
    [modified_by]                             VARCHAR (36)   NULL,
    [modified_on_behalf_by]                   VARCHAR (36)   NULL,
    [organization_id]                         VARCHAR (36)   NULL,
    [ltf_prospect_id]                         NVARCHAR (100) NULL,
    [ltf_referring_member]                    VARCHAR (36)   NULL,
    [ltf_referring_member_id]                 NVARCHAR (100) NULL,
    [ltf_send_id]                             NVARCHAR (100) NULL,
    [ltf_referring_corpacct_id]               NVARCHAR (100) NULL,
    [ltf_referring_corpacct]                  VARCHAR (36)   NULL,
    [dv_load_date_time]                       DATETIME       NOT NULL,
    [dv_r_load_source_id]                     BIGINT         NOT NULL,
    [dv_inserted_date_time]                   DATETIME       NOT NULL,
    [dv_insert_user]                          VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                    DATETIME       NULL,
    [dv_update_user]                          VARCHAR (50)   NULL,
    [dv_hash]                                 CHAR (32)      NOT NULL,
    [dv_batch_id]                             BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_ltf_campaign_instance]
    ON [dbo].[l_crmcloudsync_ltf_campaign_instance]([bk_hash] ASC, [l_crmcloudsync_ltf_campaign_instance_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_campaign_instance]([dv_batch_id] ASC);

