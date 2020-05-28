CREATE TABLE [dbo].[d_ig_ig_dimension_profit_center_dimension] (
    [d_ig_ig_dimension_profit_center_dimension_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)     NOT NULL,
    [dummy_bk_hash_key]                            CHAR (32)     NULL,
    [customer_id]                                  INT           NULL,
    [dim_cafe_profit_center_key]                   CHAR (32)     NULL,
    [ent_id]                                       INT           NULL,
    [profit_center_dim_id]                         INT           NULL,
    [profit_center_id]                             INT           NULL,
    [profit_center_name]                           NVARCHAR (50) NULL,
    [store_id]                                     INT           NULL,
    [store_name]                                   NVARCHAR (50) NULL,
    [p_ig_ig_dimension_profit_center_dimension_id] BIGINT        NOT NULL,
    [deleted_flag]                                 INT           NULL,
    [dv_load_date_time]                            DATETIME      NULL,
    [dv_load_end_date_time]                        DATETIME      NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL,
    [dv_inserted_date_time]                        DATETIME      NOT NULL,
    [dv_insert_user]                               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                         DATETIME      NULL,
    [dv_update_user]                               VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

