CREATE TABLE [dbo].[l_crmcloudsync_appointment] (
    [l_crmcloudsync_appointment_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [activity_id]                   VARCHAR (36) NULL,
    [created_by]                    VARCHAR (36) NULL,
    [created_on_behalf_by]          VARCHAR (36) NULL,
    [is_all_day_event]              VARCHAR (36) NULL,
    [is_billed]                     VARCHAR (36) NULL,
    [is_mapi_private]               VARCHAR (36) NULL,
    [is_regular_activity]           VARCHAR (36) NULL,
    [is_workflow_created]           VARCHAR (36) NULL,
    [ltf_club_appointment_sid]      VARCHAR (36) NULL,
    [ltf_club_id]                   VARCHAR (36) NULL,
    [modified_by]                   VARCHAR (36) NULL,
    [modified_on_behalf_by]         VARCHAR (36) NULL,
    [outlook_owner_appt_id]         INT          NULL,
    [owner_id]                      VARCHAR (36) NULL,
    [owning_business_unit]          VARCHAR (36) NULL,
    [owning_team]                   VARCHAR (36) NULL,
    [owning_user]                   VARCHAR (36) NULL,
    [process_id]                    VARCHAR (36) NULL,
    [regarding_object_id]           VARCHAR (36) NULL,
    [series_id]                     VARCHAR (36) NULL,
    [service_id]                    VARCHAR (36) NULL,
    [stage_id]                      VARCHAR (36) NULL,
    [transaction_currency_id]       VARCHAR (36) NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_batch_id]                   BIGINT       NOT NULL,
    [dv_r_load_source_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_hash]                       CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_crmcloudsync_appointment]
    ON [dbo].[l_crmcloudsync_appointment]([bk_hash] ASC, [l_crmcloudsync_appointment_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_appointment]([dv_batch_id] ASC);

