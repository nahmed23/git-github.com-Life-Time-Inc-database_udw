CREATE TABLE [dbo].[s_mdm_golden_record_customer] (
    [s_mdm_golden_record_customer_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [load_date_time]                  DATETIME      NULL,
    [row_number]                      INT           NULL,
    [entity_id]                       VARCHAR (128) NULL,
    [source_id]                       VARCHAR (128) NULL,
    [source_code]                     VARCHAR (128) NULL,
    [birth_date]                      VARCHAR (19)  NULL,
    [create_date]                     VARCHAR (19)  NULL,
    [terminate_date]                  VARCHAR (128) NULL,
    [email_1]                         VARCHAR (128) NULL,
    [email_2]                         VARCHAR (128) NULL,
    [sex]                             VARCHAR (128) NULL,
    [postal_address_city]             VARCHAR (50)  NULL,
    [postal_address_state]            VARCHAR (15)  NULL,
    [postal_address_line_1]           VARCHAR (75)  NULL,
    [postal_address_line_2]           VARCHAR (75)  NULL,
    [postal_address_zip_code]         VARCHAR (10)  NULL,
    [ip_address]                      VARCHAR (128) NULL,
    [first_name]                      VARCHAR (30)  NULL,
    [last_name]                       VARCHAR (75)  NULL,
    [middle_name]                     VARCHAR (30)  NULL,
    [prefix_name]                     VARCHAR (10)  NULL,
    [suffix_name]                     VARCHAR (10)  NULL,
    [phone_1]                         VARCHAR (40)  NULL,
    [phone_2]                         VARCHAR (40)  NULL,
    [update_date]                     VARCHAR (19)  NULL,
    [activation_date]                 VARCHAR (128) NULL,
    [dv_load_date_time]               DATETIME      NOT NULL,
    [dv_batch_id]                     BIGINT        NOT NULL,
    [dv_r_load_source_id]             BIGINT        NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL,
    [dv_hash]                         CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mdm_golden_record_customer]
    ON [dbo].[s_mdm_golden_record_customer]([bk_hash] ASC, [s_mdm_golden_record_customer_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mdm_golden_record_customer]([dv_batch_id] ASC);

