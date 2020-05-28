CREATE TABLE [dbo].[l_crmcloudsync_ltf_club] (
    [l_crmcloudsync_ltf_club_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)      NOT NULL,
    [created_by]                  VARCHAR (36)   NULL,
    [created_on_behalf_by]        VARCHAR (36)   NULL,
    [ltf_club_division]           VARCHAR (36)   NULL,
    [ltf_club_id]                 VARCHAR (36)   NULL,
    [ltf_club_region]             VARCHAR (36)   NULL,
    [ltf_club_regional_manager]   VARCHAR (36)   NULL,
    [ltf_clubs_id]                VARCHAR (36)   NULL,
    [ltf_club_team_id]            VARCHAR (36)   NULL,
    [ltf_general_manager]         VARCHAR (36)   NULL,
    [ltf_mem]                     VARCHAR (36)   NULL,
    [ltf_mms_club_id]             NVARCHAR (10)  NULL,
    [ltf_npt_rep]                 VARCHAR (36)   NULL,
    [ltf_pdth]                    VARCHAR (36)   NULL,
    [ltf_udw_id]                  NVARCHAR (255) NULL,
    [ltf_web_specialist_team]     VARCHAR (36)   NULL,
    [modified_by]                 VARCHAR (36)   NULL,
    [modified_on_behalf_by]       VARCHAR (36)   NULL,
    [organization_id]             VARCHAR (36)   NULL,
    [ltf_territory_id]            VARCHAR (36)   NULL,
    [ltf_parent_territory_id]     VARCHAR (36)   NULL,
    [ltf_area_director]           VARCHAR (36)   NULL,
    [ltf_cig_user]                VARCHAR (36)   NULL,
    [ltf_old_team]                VARCHAR (36)   NULL,
    [ltf_regional_sales_lead]     VARCHAR (36)   NULL,
    [ltf_regional_vice_president] VARCHAR (36)   NULL,
    [ltf_sr_mem]                  VARCHAR (36)   NULL,
    [dv_load_date_time]           DATETIME       NOT NULL,
    [dv_r_load_source_id]         BIGINT         NOT NULL,
    [dv_inserted_date_time]       DATETIME       NOT NULL,
    [dv_insert_user]              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]        DATETIME       NULL,
    [dv_update_user]              VARCHAR (50)   NULL,
    [dv_hash]                     CHAR (32)      NOT NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_ltf_club]([dv_batch_id] ASC);

