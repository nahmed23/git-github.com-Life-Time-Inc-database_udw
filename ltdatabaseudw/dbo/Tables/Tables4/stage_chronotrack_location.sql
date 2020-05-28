CREATE TABLE [dbo].[stage_chronotrack_location] (
    [stage_chronotrack_location_id] BIGINT          NOT NULL,
    [id]                            BIGINT          NULL,
    [name]                          NVARCHAR (255)  NULL,
    [latitude]                      DECIMAL (26, 6) NULL,
    [longitude]                     DECIMAL (26, 6) NULL,
    [time_zone]                     NVARCHAR (30)   NULL,
    [street]                        NVARCHAR (255)  NULL,
    [street2]                       NVARCHAR (255)  NULL,
    [city]                          NVARCHAR (255)  NULL,
    [region_id]                     NCHAR (6)       NULL,
    [county]                        NVARCHAR (255)  NULL,
    [postal_code]                   NVARCHAR (20)   NULL,
    [ctime]                         INT             NULL,
    [mtime]                         INT             NULL,
    [dummy_modified_date_time]      DATETIME        NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

