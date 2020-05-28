CREATE TABLE [dbo].[d_fitmetrix_api_facility_location_id_resources] (
    [d_fitmetrix_api_facility_location_id_resources_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)     NOT NULL,
    [dim_fitmetrix_location_resource_key]               CHAR (32)     NULL,
    [facility_location_resource_id]                     INT           NULL,
    [boss_resource_id]                                  INT           NULL,
    [dim_fitmetrix_location_key]                        CHAR (32)     NULL,
    [max_capacity]                                      INT           NULL,
    [resource_name]                                     VARCHAR (256) NULL,
    [p_fitmetrix_api_facility_location_id_resources_id] BIGINT        NOT NULL,
    [deleted_flag]                                      INT           NULL,
    [dv_load_date_time]                                 DATETIME      NULL,
    [dv_load_end_date_time]                             DATETIME      NULL,
    [dv_batch_id]                                       BIGINT        NOT NULL,
    [dv_inserted_date_time]                             DATETIME      NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                              DATETIME      NULL,
    [dv_update_user]                                    VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

