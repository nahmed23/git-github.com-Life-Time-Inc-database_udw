CREATE TABLE [dbo].[dim_mdm_golden_record_customer_id_list] (
    [dim_mdm_golden_record_customer_id_list_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [dim_mdm_golden_record_customer_id_list_key] CHAR (32)    NULL,
    [entity_id]                                  BIGINT       NULL,
    [dim_description_key]                        VARCHAR (50) NULL,
    [id]                                         VARCHAR (50) NULL,
    [id_type]                                    INT          NULL,
    [mdm_load_date_time]                         DATETIME     NULL,
    [udw_load_date_time]                         DATETIME     NULL,
    [dv_load_date_time]                          DATETIME     NULL,
    [dv_load_end_date_time]                      DATETIME     NULL,
    [dv_batch_id]                                BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_mdm_golden_record_customer_id_list_key]));

