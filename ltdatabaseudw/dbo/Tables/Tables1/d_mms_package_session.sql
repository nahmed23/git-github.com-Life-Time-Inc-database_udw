CREATE TABLE [dbo].[d_mms_package_session] (
    [d_mms_package_session_id]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [fact_mms_package_session_key]  CHAR (32)      NULL,
    [package_session_id]            INT            NULL,
    [comment]                       VARCHAR (255)  NULL,
    [created_dim_date_key]          CHAR (8)       NULL,
    [created_dim_time_key]          CHAR (8)       NULL,
    [delivered_dim_club_key]        CHAR (32)      NULL,
    [delivered_dim_date_key]        CHAR (8)       NULL,
    [delivered_dim_employee_key]    CHAR (32)      NULL,
    [delivered_dim_team_member_key] CHAR (32)      NULL,
    [delivered_dim_time_key]        CHAR (8)       NULL,
    [delivered_session_price]       DECIMAL (9, 4) NULL,
    [fact_mms_package_key]          CHAR (32)      NULL,
    [package_id]                    INT            NULL,
    [package_session_club_key]      CHAR (32)      NULL,
    [p_mms_package_session_id]      BIGINT         NOT NULL,
    [dv_load_date_time]             DATETIME       NULL,
    [dv_load_end_date_time]         DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_package_session]([dv_batch_id] ASC);

