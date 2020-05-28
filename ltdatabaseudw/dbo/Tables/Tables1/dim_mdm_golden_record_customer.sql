CREATE TABLE [dbo].[dim_mdm_golden_record_customer] (
    [dim_mdm_golden_record_customer_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_mdm_golden_record_customer_key] CHAR (32)     NULL,
    [entity_id]                          BIGINT        NULL,
    [birth_date]                         DATETIME      NULL,
    [email_1]                            VARCHAR (128) NULL,
    [first_name]                         VARCHAR (30)  NULL,
    [former_member_flag]                 CHAR (1)      NULL,
    [last_name]                          VARCHAR (75)  NULL,
    [middle_name]                        VARCHAR (30)  NULL,
    [phone_1]                            VARCHAR (40)  NULL,
    [postal_address_city]                VARCHAR (50)  NULL,
    [postal_address_line_1]              VARCHAR (75)  NULL,
    [postal_address_line_2]              VARCHAR (75)  NULL,
    [postal_address_state]               VARCHAR (15)  NULL,
    [postal_address_zip_code]            VARCHAR (10)  NULL,
    [prefix_name]                        VARCHAR (10)  NULL,
    [sex]                                VARCHAR (128) NULL,
    [suffix_name]                        VARCHAR (10)  NULL,
    [dv_load_date_time]                  DATETIME      NULL,
    [dv_load_end_date_time]              DATETIME      NULL,
    [dv_batch_id]                        BIGINT        NULL,
    [dv_inserted_date_time]              DATETIME      NOT NULL,
    [dv_insert_user]                     VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]               DATETIME      NULL,
    [dv_update_user]                     VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_mdm_golden_record_customer_key]));


GO
CREATE CLUSTERED INDEX [ci_dim_mdm_golden_record_customer]
    ON [dbo].[dim_mdm_golden_record_customer]([entity_id] ASC);

