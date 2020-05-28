CREATE TABLE [dbo].[s_fitmetrix_api_facility_location_id_resources] (
    [s_fitmetrix_api_facility_location_id_resources_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)      NOT NULL,
    [facility_location_resource_id]                     INT            NULL,
    [max_capacity]                                      INT            NULL,
    [name]                                              VARCHAR (255)  NULL,
    [configuration]                                     VARCHAR (8000) NULL,
    [use_intervals]                                     VARCHAR (255)  NULL,
    [address]                                           VARCHAR (255)  NULL,
    [lat]                                               INT            NULL,
    [long]                                              INT            NULL,
    [dummy_modified_date_time]                          DATETIME       NULL,
    [dv_load_date_time]                                 DATETIME       NOT NULL,
    [dv_r_load_source_id]                               BIGINT         NOT NULL,
    [dv_inserted_date_time]                             DATETIME       NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                              DATETIME       NULL,
    [dv_update_user]                                    VARCHAR (50)   NULL,
    [dv_hash]                                           CHAR (32)      NOT NULL,
    [dv_batch_id]                                       BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_fitmetrix_api_facility_location_id_resources]([dv_batch_id] ASC);

