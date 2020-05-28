CREATE TABLE [dbo].[stage_exerp_center] (
    [stage_exerp_center_id]    BIGINT          NOT NULL,
    [id]                       INT             NULL,
    [state]                    VARCHAR (4000)  NULL,
    [county]                   VARCHAR (4000)  NULL,
    [manager_person_id]        VARCHAR (4000)  NULL,
    [time_zone]                VARCHAR (4000)  NULL,
    [city]                     VARCHAR (4000)  NULL,
    [phone_number]             VARCHAR (4000)  NULL,
    [address3]                 VARCHAR (4000)  NULL,
    [address2]                 VARCHAR (4000)  NULL,
    [address1]                 VARCHAR (4000)  NULL,
    [postal_code]              VARCHAR (4000)  NULL,
    [country_code]             VARCHAR (4000)  NULL,
    [shortname]                VARCHAR (4000)  NULL,
    [name]                     VARCHAR (4000)  NULL,
    [migration_date]           DATETIME        NULL,
    [startup_date]             DATETIME        NULL,
    [longitude]                DECIMAL (26, 6) NULL,
    [latitude]                 DECIMAL (26, 6) NULL,
    [external_id]              VARCHAR (4000)  NULL,
    [dummy_modified_date_time] DATETIME        NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

