CREATE TABLE [dbo].[s_chronotrack_location] (
    [s_chronotrack_location_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [location_id]               BIGINT          NULL,
    [name]                      NVARCHAR (255)  NULL,
    [latitude]                  DECIMAL (26, 6) NULL,
    [longitude]                 DECIMAL (26, 6) NULL,
    [time_zone]                 NVARCHAR (30)   NULL,
    [street]                    NVARCHAR (255)  NULL,
    [street_2]                  NVARCHAR (255)  NULL,
    [city]                      NVARCHAR (255)  NULL,
    [county]                    NVARCHAR (255)  NULL,
    [postal_code]               NVARCHAR (20)   NULL,
    [create_time]               INT             NULL,
    [modified_time]             INT             NULL,
    [dummy_modified_date_time]  DATETIME        NULL,
    [dv_load_date_time]         DATETIME        NOT NULL,
    [dv_r_load_source_id]       BIGINT          NOT NULL,
    [dv_inserted_date_time]     DATETIME        NOT NULL,
    [dv_insert_user]            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]      DATETIME        NULL,
    [dv_update_user]            VARCHAR (50)    NULL,
    [dv_hash]                   CHAR (32)       NOT NULL,
    [dv_deleted]                BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

