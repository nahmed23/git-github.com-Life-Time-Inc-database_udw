CREATE TABLE [dbo].[s_crmcloudsync_ltf_campaign_instance] (
    [s_crmcloudsync_ltf_campaign_instance_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)      NOT NULL,
    [created_by_name]                         NVARCHAR (200) NULL,
    [created_by_yomi_name]                    NVARCHAR (200) NULL,
    [created_on]                              DATETIME       NULL,
    [created_on_behalf_by_name]               NVARCHAR (200) NULL,
    [created_on_behalf_by_yomi_name]          NVARCHAR (200) NULL,
    [import_sequence_number]                  INT            NULL,
    [ltf_campaign_instance_id]                VARCHAR (36)   NULL,
    [ltf_campaign_name]                       NVARCHAR (128) NULL,
    [ltf_club_name]                           NVARCHAR (100) NULL,
    [ltf_connect_witham]                      BIT            NULL,
    [ltf_connect_witham_name]                 NVARCHAR (255) NULL,
    [ltf_expiration_date]                     DATETIME       NULL,
    [ltf_initial_use_date]                    DATETIME       NULL,
    [ltf_issued_by_name]                      NVARCHAR (200) NULL,
    [ltf_issued_by_yomi_name]                 NVARCHAR (200) NULL,
    [ltf_issued_date]                         DATETIME       NULL,
    [ltf_issued_days]                         INT            NULL,
    [ltf_issuing_contact_name]                NVARCHAR (160) NULL,
    [ltf_issuing_contact_yomi_name]           NVARCHAR (160) NULL,
    [ltf_issuing_lead_name]                   NVARCHAR (160) NULL,
    [ltf_issuing_lead_yomi_name]              NVARCHAR (160) NULL,
    [ltf_issuing_opportunity_name]            NVARCHAR (300) NULL,
    [ltf_name]                                NVARCHAR (100) NULL,
    [ltf_pass_begin_date]                     DATETIME       NULL,
    [ltf_qr_code]                             VARCHAR (8000) NULL,
    [ltf_remaining_days]                      INT            NULL,
    [modified_by_name]                        NVARCHAR (200) NULL,
    [modified_by_yomi_name]                   NVARCHAR (200) NULL,
    [modified_on]                             DATETIME       NULL,
    [modified_on_behalf_by_name]              NVARCHAR (200) NULL,
    [modified_on_behalf_by_yomi_name]         NVARCHAR (200) NULL,
    [organization_id_name]                    NVARCHAR (160) NULL,
    [overridden_created_on]                   DATETIME       NULL,
    [state_code]                              INT            NULL,
    [state_code_name]                         NVARCHAR (255) NULL,
    [status_code]                             INT            NULL,
    [status_code_name]                        NVARCHAR (255) NULL,
    [time_zone_rule_version_number]           INT            NULL,
    [utc_conversion_time_zone_code]           INT            NULL,
    [version_number]                          BIGINT         NULL,
    [inserted_date_time]                      DATETIME       NULL,
    [insert_user]                             VARCHAR (50)   NULL,
    [updated_date_time]                       DATETIME       NULL,
    [update_user]                             VARCHAR (50)   NULL,
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
CREATE CLUSTERED INDEX [ci_s_crmcloudsync_ltf_campaign_instance]
    ON [dbo].[s_crmcloudsync_ltf_campaign_instance]([bk_hash] ASC, [s_crmcloudsync_ltf_campaign_instance_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_ltf_campaign_instance]([dv_batch_id] ASC);

