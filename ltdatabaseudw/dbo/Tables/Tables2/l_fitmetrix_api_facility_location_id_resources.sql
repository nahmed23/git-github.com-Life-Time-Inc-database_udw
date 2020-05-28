CREATE TABLE [dbo].[l_fitmetrix_api_facility_location_id_resources] (
    [l_fitmetrix_api_facility_location_id_resources_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)       NOT NULL,
    [facility_location_resource_id]                     INT             NULL,
    [facility_location_id]                              INT             NULL,
    [external_id]                                       VARCHAR (255)   NULL,
    [default_activity_type_id]                          INT             NULL,
    [external_id_base64_decoded]                        VARBINARY (255) NULL,
    [dv_load_date_time]                                 DATETIME        NOT NULL,
    [dv_r_load_source_id]                               BIGINT          NOT NULL,
    [dv_inserted_date_time]                             DATETIME        NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                              DATETIME        NULL,
    [dv_update_user]                                    VARCHAR (50)    NULL,
    [dv_hash]                                           CHAR (32)       NOT NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_fitmetrix_api_facility_location_id_resources]([dv_batch_id] ASC);

