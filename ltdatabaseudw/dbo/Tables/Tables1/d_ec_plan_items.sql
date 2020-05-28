CREATE TABLE [dbo].[d_ec_plan_items] (
    [d_ec_plan_items_id]                    BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [fact_trainerize_program_plan_item_key] VARCHAR (32)    NULL,
    [plan_item_id]                          INT             NULL,
    [completed_flag]                        CHAR (1)        NULL,
    [created_dim_date_key]                  VARCHAR (8)     NULL,
    [dim_trainerize_plan_key]               VARCHAR (32)    NULL,
    [item_description]                      NVARCHAR (4000) NULL,
    [item_dim_date_key]                     VARCHAR (8)     NULL,
    [item_dim_time_key]                     INT             NULL,
    [item_name]                             NVARCHAR (4000) NULL,
    [item_type]                             INT             NULL,
    [source_id]                             NVARCHAR (50)   NULL,
    [source_type]                           INT             NULL,
    [updated_dim_date_key]                  VARCHAR (8)     NULL,
    [p_ec_plan_items_id]                    BIGINT          NOT NULL,
    [deleted_flag]                          INT             NULL,
    [dv_load_date_time]                     DATETIME        NULL,
    [dv_load_end_date_time]                 DATETIME        NULL,
    [dv_batch_id]                           BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

