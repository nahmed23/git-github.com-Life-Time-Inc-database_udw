CREATE TABLE [dbo].[s_hybris_point_of_service] (
    [s_hybris_point_of_service_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)       NOT NULL,
    [hjmpts]                         BIGINT          NULL,
    [created_ts]                     DATETIME        NULL,
    [modified_ts]                    DATETIME        NULL,
    [point_of_service_pk]            BIGINT          NULL,
    [p_name]                         NVARCHAR (255)  NULL,
    [p_description]                  NVARCHAR (255)  NULL,
    [p_mapicon]                      BIGINT          NULL,
    [p_latitude]                     DECIMAL (26, 6) NULL,
    [p_longitude]                    DECIMAL (26, 6) NULL,
    [p_geo_code_time_stamp]          DATETIME        NULL,
    [p_opening_schedule]             BIGINT          NULL,
    [p_store_image]                  BIGINT          NULL,
    [p_display_name]                 NVARCHAR (255)  NULL,
    [p_nearby_store_radius]          DECIMAL (26, 6) NULL,
    [p_next_month_dues_flag]         TINYINT         NULL,
    [p_next_month_dues_day_of_month] INT             NULL,
    [p_active_flag]                  TINYINT         NULL,
    [acl_ts]                         BIGINT          NULL,
    [prop_ts]                        BIGINT          NULL,
    [dv_load_date_time]              DATETIME        NOT NULL,
    [dv_r_load_source_id]            BIGINT          NOT NULL,
    [dv_inserted_date_time]          DATETIME        NOT NULL,
    [dv_insert_user]                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]           DATETIME        NULL,
    [dv_update_user]                 VARCHAR (50)    NULL,
    [dv_hash]                        CHAR (32)       NOT NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_point_of_service]
    ON [dbo].[s_hybris_point_of_service]([bk_hash] ASC, [s_hybris_point_of_service_id] ASC);

