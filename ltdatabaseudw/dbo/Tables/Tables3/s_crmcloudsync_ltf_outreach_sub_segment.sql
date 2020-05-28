CREATE TABLE [dbo].[s_crmcloudsync_ltf_outreach_sub_segment] (
    [s_crmcloudsync_ltf_outreach_sub_segment_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)      NOT NULL,
    [created_by_name]                            NVARCHAR (200) NULL,
    [created_by_yomi_name]                       NVARCHAR (200) NULL,
    [created_on]                                 DATETIME       NULL,
    [created_on_behalf_by_name]                  NVARCHAR (200) NULL,
    [created_on_behalf_by_yomi_name]             NVARCHAR (200) NULL,
    [import_sequence_number]                     INT            NULL,
    [ltf_attribute_index]                        INT            NULL,
    [ltf_description]                            NVARCHAR (100) NULL,
    [ltf_outreach_sub_segment_id]                VARCHAR (36)   NULL,
    [ltf_subsegment]                             NVARCHAR (25)  NULL,
    [modified_by_name]                           NVARCHAR (200) NULL,
    [modified_by_yomi_name]                      NVARCHAR (200) NULL,
    [modified_on]                                DATETIME       NULL,
    [modified_on_behalf_by_name]                 NVARCHAR (200) NULL,
    [modified_on_behalf_by_yomi_name]            NVARCHAR (200) NULL,
    [overridden_created_on]                      DATETIME       NULL,
    [owner_id_name]                              NVARCHAR (200) NULL,
    [owner_id_type]                              NVARCHAR (64)  NULL,
    [owner_id_yomi_name]                         NVARCHAR (200) NULL,
    [state_code]                                 INT            NULL,
    [state_code_name]                            NVARCHAR (255) NULL,
    [status_code]                                INT            NULL,
    [status_code_name]                           NVARCHAR (255) NULL,
    [time_zone_rule_version_number]              INT            NULL,
    [utc_conversion_time_zone_code]              INT            NULL,
    [version_number]                             BIGINT         NULL,
    [inserted_date_time]                         DATETIME       NULL,
    [insert_user]                                VARCHAR (100)  NULL,
    [updated_date_time]                          DATETIME       NULL,
    [update_user]                                VARCHAR (50)   NULL,
    [dv_load_date_time]                          DATETIME       NOT NULL,
    [dv_r_load_source_id]                        BIGINT         NOT NULL,
    [dv_inserted_date_time]                      DATETIME       NOT NULL,
    [dv_insert_user]                             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                       DATETIME       NULL,
    [dv_update_user]                             VARCHAR (50)   NULL,
    [dv_hash]                                    CHAR (32)      NOT NULL,
    [dv_batch_id]                                BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_crmcloudsync_ltf_outreach_sub_segment]
    ON [dbo].[s_crmcloudsync_ltf_outreach_sub_segment]([bk_hash] ASC, [s_crmcloudsync_ltf_outreach_sub_segment_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_ltf_outreach_sub_segment]([dv_batch_id] ASC);

