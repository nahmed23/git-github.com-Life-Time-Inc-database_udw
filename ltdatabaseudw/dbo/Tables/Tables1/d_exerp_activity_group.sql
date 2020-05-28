CREATE TABLE [dbo].[d_exerp_activity_group] (
    [d_exerp_activity_group_id]             BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)      NOT NULL,
    [dim_exerp_activity_group_key]          VARCHAR (32)   NULL,
    [activity_group_id]                     INT            NULL,
    [activity_group_name]                   VARCHAR (4000) NULL,
    [activity_group_state]                  VARCHAR (4000) NULL,
    [book_api_flag]                         CHAR (1)       NULL,
    [book_client_flag]                      CHAR (1)       NULL,
    [book_kiosk_flag]                       CHAR (1)       NULL,
    [book_mobile_api_flag]                  CHAR (1)       NULL,
    [book_web_flag]                         CHAR (1)       NULL,
    [external_id]                           VARCHAR (200)  NULL,
    [parent_d_exerp_activity_group_bk_hash] VARCHAR (32)   NULL,
    [p_exerp_activity_group_id]             BIGINT         NOT NULL,
    [deleted_flag]                          INT            NULL,
    [dv_load_date_time]                     DATETIME       NULL,
    [dv_load_end_date_time]                 DATETIME       NULL,
    [dv_batch_id]                           BIGINT         NOT NULL,
    [dv_inserted_date_time]                 DATETIME       NOT NULL,
    [dv_insert_user]                        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                  DATETIME       NULL,
    [dv_update_user]                        VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

