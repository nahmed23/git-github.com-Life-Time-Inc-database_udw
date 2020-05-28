CREATE TABLE [dbo].[d_fitmetrix_api_activities] (
    [d_fitmetrix_api_activities_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)     NOT NULL,
    [dim_fitmetrix_activity_key]    CHAR (32)     NULL,
    [activity_id]                   INT           NULL,
    [activity_added_dim_date_key]   CHAR (32)     NULL,
    [activity_name]                 VARCHAR (255) NULL,
    [activity_type_id]              INT           NULL,
    [external_id]                   VARCHAR (255) NULL,
    [is_deleted_flag]               VARCHAR (255) NULL,
    [position]                      INT           NULL,
    [p_fitmetrix_api_activities_id] BIGINT        NOT NULL,
    [deleted_flag]                  INT           NULL,
    [dv_load_date_time]             DATETIME      NULL,
    [dv_load_end_date_time]         DATETIME      NULL,
    [dv_batch_id]                   BIGINT        NOT NULL,
    [dv_inserted_date_time]         DATETIME      NOT NULL,
    [dv_insert_user]                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]          DATETIME      NULL,
    [dv_update_user]                VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_fitmetrix_api_activities]([dv_batch_id] ASC);

