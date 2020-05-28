CREATE TABLE [dbo].[s_fitmetrix_api_activities] (
    [s_fitmetrix_api_activities_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)     NOT NULL,
    [activity_id]                   INT           NULL,
    [activity_name]                 VARCHAR (255) NULL,
    [activity_added]                VARCHAR (255) NULL,
    [level]                         VARCHAR (255) NULL,
    [need_loaners]                  VARCHAR (255) NULL,
    [check_in_type]                 VARCHAR (255) NULL,
    [allow_reservation]             VARCHAR (255) NULL,
    [non_integration_sync]          VARCHAR (255) NULL,
    [icon]                          VARCHAR (255) NULL,
    [is_assessment]                 VARCHAR (255) NULL,
    [image]                         VARCHAR (255) NULL,
    [is_deleted]                    VARCHAR (255) NULL,
    [is_appointment_activity]       VARCHAR (255) NULL,
    [app_name]                      VARCHAR (255) NULL,
    [position]                      INT           NULL,
    [delete]                        VARCHAR (255) NULL,
    [app_image]                     VARCHAR (255) NULL,
    [app_icon]                      VARCHAR (255) NULL,
    [is_manual_attendance]          VARCHAR (255) NULL,
    [duration_minutes]              INT           NULL,
    [dummy_modified_date_time]      DATETIME      NULL,
    [dv_load_date_time]             DATETIME      NOT NULL,
    [dv_r_load_source_id]           BIGINT        NOT NULL,
    [dv_inserted_date_time]         DATETIME      NOT NULL,
    [dv_insert_user]                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]          DATETIME      NULL,
    [dv_update_user]                VARCHAR (50)  NULL,
    [dv_hash]                       CHAR (32)     NOT NULL,
    [dv_batch_id]                   BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_fitmetrix_api_activities]([dv_batch_id] ASC);

