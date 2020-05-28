CREATE TABLE [dbo].[s_mms_package_session] (
    [s_mms_package_session_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [package_session_id]       INT            NULL,
    [created_date_time]        DATETIME       NULL,
    [utc_created_date_time]    DATETIME       NULL,
    [created_date_time_zone]   VARCHAR (4)    NULL,
    [modified_date_time]       DATETIME       NULL,
    [utc_modified_date_time]   DATETIME       NULL,
    [modified_date_time_zone]  VARCHAR (4)    NULL,
    [delivered_date_time]      DATETIME       NULL,
    [utc_delivered_date_time]  DATETIME       NULL,
    [delivered_date_time_zone] VARCHAR (4)    NULL,
    [session_price]            DECIMAL (9, 4) NULL,
    [comment]                  VARCHAR (255)  NULL,
    [inserted_date_time]       DATETIME       NULL,
    [updated_date_time]        DATETIME       NULL,
    [dv_load_date_time]        DATETIME       NOT NULL,
    [dv_batch_id]              BIGINT         NOT NULL,
    [dv_r_load_source_id]      BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_hash]                  CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_package_session]
    ON [dbo].[s_mms_package_session]([bk_hash] ASC, [s_mms_package_session_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_package_session]([dv_batch_id] ASC);

