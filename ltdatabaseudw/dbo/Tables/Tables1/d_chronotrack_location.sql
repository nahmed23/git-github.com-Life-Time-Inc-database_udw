CREATE TABLE [dbo].[d_chronotrack_location] (
    [d_chronotrack_location_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [location_id]               BIGINT          NULL,
    [city]                      NVARCHAR (255)  NULL,
    [county]                    NVARCHAR (255)  NULL,
    [create_time]               INT             NULL,
    [latitude]                  DECIMAL (26, 6) NULL,
    [longitude]                 DECIMAL (26, 6) NULL,
    [modified_time]             INT             NULL,
    [name]                      NVARCHAR (255)  NULL,
    [postal_code]               NVARCHAR (20)   NULL,
    [region_id]                 NCHAR (6)       NULL,
    [street]                    NVARCHAR (255)  NULL,
    [street_2]                  NVARCHAR (255)  NULL,
    [time_zone]                 NVARCHAR (30)   NULL,
    [p_chronotrack_location_id] BIGINT          NOT NULL,
    [deleted_flag]              INT             NULL,
    [dv_load_date_time]         DATETIME        NULL,
    [dv_load_end_date_time]     DATETIME        NULL,
    [dv_batch_id]               BIGINT          NOT NULL,
    [dv_inserted_date_time]     DATETIME        NOT NULL,
    [dv_insert_user]            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]      DATETIME        NULL,
    [dv_update_user]            VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

