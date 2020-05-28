CREATE TABLE [dbo].[d_exerp_center] (
    [d_exerp_center_id]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [center_id]                INT             NULL,
    [center_name]              VARCHAR (4000)  NULL,
    [county]                   VARCHAR (4000)  NULL,
    [external_id]              VARCHAR (4000)  NULL,
    [latitude]                 DECIMAL (26, 6) NULL,
    [longitude]                DECIMAL (26, 6) NULL,
    [manager_dim_employee_key] VARCHAR (32)    NULL,
    [migration_dim_date_key]   CHAR (8)        NULL,
    [startup_dim_date_key]     CHAR (8)        NULL,
    [time_zone]                VARCHAR (4000)  NULL,
    [p_exerp_center_id]        BIGINT          NOT NULL,
    [deleted_flag]             INT             NULL,
    [dv_load_date_time]        DATETIME        NULL,
    [dv_load_end_date_time]    DATETIME        NULL,
    [dv_batch_id]              BIGINT          NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

