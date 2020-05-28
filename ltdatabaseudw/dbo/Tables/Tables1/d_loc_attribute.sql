CREATE TABLE [dbo].[d_loc_attribute] (
    [d_loc_attribute_id]               BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)     NOT NULL,
    [attribute_id]                     BIGINT        NULL,
    [attribute_value]                  VARCHAR (100) NULL,
    [business_key]                     VARCHAR (32)  NULL,
    [business_source_name]             VARCHAR (100) NULL,
    [created_by]                       VARCHAR (100) NULL,
    [created_dim_date_key]             VARCHAR (8)   NULL,
    [d_loc_val_attribute_type_bk_hash] VARCHAR (32)  NULL,
    [deleted_by]                       VARCHAR (100) NULL,
    [dim_location_key]                 VARCHAR (32)  NULL,
    [managed_by_udw_flag]              CHAR (1)      NULL,
    [updated_by]                       VARCHAR (100) NULL,
    [updated_dim_date_key]             VARCHAR (8)   NULL,
    [p_loc_attribute_id]               BIGINT        NOT NULL,
    [deleted_flag]                     INT           NULL,
    [dv_load_date_time]                DATETIME      NULL,
    [dv_load_end_date_time]            DATETIME      NULL,
    [dv_batch_id]                      BIGINT        NOT NULL,
    [dv_inserted_date_time]            DATETIME      NOT NULL,
    [dv_insert_user]                   VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]             DATETIME      NULL,
    [dv_update_user]                   VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

