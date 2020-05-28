CREATE TABLE [dbo].[d_exerp_clipcard_usage] (
    [d_exerp_clipcard_usage_id]               BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)      NOT NULL,
    [clipcard_usage_id]                       INT            NULL,
    [cancelled_flag]                          CHAR (1)       NULL,
    [clipcard_usage_entered_dim_employee_key] VARCHAR (32)   NULL,
    [clipcard_usage_state]                    VARCHAR (4000) NULL,
    [clipcard_usage_type]                     VARCHAR (4000) NULL,
    [clips]                                   INT            NULL,
    [commission_units]                        INT            NULL,
    [delivered_dim_club_key]                  CHAR (32)      NULL,
    [dim_exerp_clipcard_key]                  VARCHAR (32)   NULL,
    [usage_dim_date_key]                      VARCHAR (8)    NULL,
    [usage_dim_time_key]                      INT            NULL,
    [p_exerp_clipcard_usage_id]               BIGINT         NOT NULL,
    [deleted_flag]                            INT            NULL,
    [dv_load_date_time]                       DATETIME       NULL,
    [dv_load_end_date_time]                   DATETIME       NULL,
    [dv_batch_id]                             BIGINT         NOT NULL,
    [dv_inserted_date_time]                   DATETIME       NOT NULL,
    [dv_insert_user]                          VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                    DATETIME       NULL,
    [dv_update_user]                          VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

