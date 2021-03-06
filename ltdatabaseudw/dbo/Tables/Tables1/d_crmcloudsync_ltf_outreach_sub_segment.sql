﻿CREATE TABLE [dbo].[d_crmcloudsync_ltf_outreach_sub_segment] (
    [d_crmcloudsync_ltf_outreach_sub_segment_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)      NOT NULL,
    [dim_crm_ltf_outreach_sub_segment_key]       VARCHAR (32)   NULL,
    [ltf_outreach_sub_segment_id]                NVARCHAR (200) NULL,
    [created_by_dim_crm_system_user_key]         VARCHAR (32)   NULL,
    [created_by_name]                            NVARCHAR (200) NULL,
    [created_dim_date_key]                       VARCHAR (8)    NULL,
    [created_dim_time_key]                       INT            NULL,
    [created_on]                                 DATETIME       NULL,
    [import_sequence_number]                     INT            NULL,
    [insert_user]                                NVARCHAR (100) NULL,
    [inserted_date_time]                         DATETIME       NULL,
    [inserted_dim_date_key]                      VARCHAR (8)    NULL,
    [inserted_dim_time_key]                      INT            NULL,
    [ltf_attribute_index]                        INT            NULL,
    [ltf_description]                            NVARCHAR (100) NULL,
    [ltf_subsegment]                             NVARCHAR (25)  NULL,
    [modified_by_dim_crm_system_user_key]        VARCHAR (32)   NULL,
    [modified_by_name]                           NVARCHAR (200) NULL,
    [modified_dim_date_key]                      VARCHAR (8)    NULL,
    [modified_dim_time_key]                      INT            NULL,
    [modified_on]                                DATETIME       NULL,
    [state_code]                                 INT            NULL,
    [state_code_name]                            NVARCHAR (255) NULL,
    [status_code]                                INT            NULL,
    [status_code_name]                           NVARCHAR (255) NULL,
    [update_user]                                NVARCHAR (50)  NULL,
    [updated_date_time]                          DATETIME       NULL,
    [updated_dim_date_key]                       VARCHAR (8)    NULL,
    [updated_dim_time_key]                       INT            NULL,
    [version_number]                             BIGINT         NULL,
    [p_crmcloudsync_ltf_outreach_sub_segment_id] BIGINT         NOT NULL,
    [deleted_flag]                               INT            NULL,
    [dv_load_date_time]                          DATETIME       NULL,
    [dv_load_end_date_time]                      DATETIME       NULL,
    [dv_batch_id]                                BIGINT         NOT NULL,
    [dv_inserted_date_time]                      DATETIME       NOT NULL,
    [dv_insert_user]                             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                       DATETIME       NULL,
    [dv_update_user]                             VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

