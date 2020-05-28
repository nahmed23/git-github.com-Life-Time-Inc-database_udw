CREATE TABLE [dbo].[dim_mdm_golden_record_customer_phone_list] (
    [dim_mdm_golden_record_customer_phone_list_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_mdm_golden_record_customer_phone_list_key] CHAR (32)     NULL,
    [entity_id]                                     BIGINT        NULL,
    [phone]                                         VARCHAR (128) NULL,
    [type]                                          VARCHAR (50)  NULL,
    [dv_load_date_time]                             DATETIME      NULL,
    [dv_load_end_date_time]                         DATETIME      NULL,
    [dv_batch_id]                                   BIGINT        NULL,
    [dv_inserted_date_time]                         DATETIME      NOT NULL,
    [dv_insert_user]                                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                          DATETIME      NULL,
    [dv_update_user]                                VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_mdm_golden_record_customer_phone_list_key]));


GO
CREATE CLUSTERED INDEX [ci_dim_mdm_golden_record_customer_phone_list]
    ON [dbo].[dim_mdm_golden_record_customer_phone_list]([entity_id] ASC, [type] ASC);

