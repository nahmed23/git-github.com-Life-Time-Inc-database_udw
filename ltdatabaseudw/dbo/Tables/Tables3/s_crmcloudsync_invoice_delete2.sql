﻿CREATE TABLE [dbo].[s_crmcloudsync_invoice_delete2] (
    [s_crmcloudsync_invoice_id]            BIGINT          NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [account_id_name]                      NVARCHAR (160)  NULL,
    [account_id_yomi_name]                 NVARCHAR (160)  NULL,
    [bill_to_city]                         NVARCHAR (80)   NULL,
    [bill_to_composite]                    VARCHAR (8000)  NULL,
    [bill_to_country]                      NVARCHAR (80)   NULL,
    [bill_to_fax]                          NVARCHAR (50)   NULL,
    [bill_to_line_1]                       NVARCHAR (250)  NULL,
    [bill_to_line_2]                       NVARCHAR (250)  NULL,
    [bill_to_line_3]                       NVARCHAR (250)  NULL,
    [bill_to_name]                         NVARCHAR (200)  NULL,
    [bill_to_postal_code]                  NVARCHAR (20)   NULL,
    [bill_to_state_or_province]            NVARCHAR (50)   NULL,
    [bill_to_telephone]                    NVARCHAR (50)   NULL,
    [contact_id_name]                      NVARCHAR (160)  NULL,
    [contact_id_yomi_name]                 NVARCHAR (160)  NULL,
    [created_by_name]                      NVARCHAR (200)  NULL,
    [created_by_yomi_name]                 NVARCHAR (200)  NULL,
    [created_on]                           DATETIME        NULL,
    [created_on_behalf_by_name]            NVARCHAR (200)  NULL,
    [created_on_behalf_by_yomi_name]       NVARCHAR (200)  NULL,
    [customer_id_name]                     NVARCHAR (160)  NULL,
    [customer_id_type]                     NVARCHAR (64)   NULL,
    [customer_id_yomi_name]                NVARCHAR (450)  NULL,
    [date_delivered]                       DATETIME        NULL,
    [description]                          VARCHAR (8000)  NULL,
    [discount_amount]                      DECIMAL (26, 3) NULL,
    [discount_amount_base]                 DECIMAL (26, 3) NULL,
    [discount_percentage]                  DECIMAL (28)    NULL,
    [due_date]                             DATETIME        NULL,
    [entity_image_time_stamp]              BIGINT          NULL,
    [entity_image_url]                     NVARCHAR (200)  NULL,
    [exchange_rate]                        DECIMAL (28)    NULL,
    [freight_amount]                       DECIMAL (26, 3) NULL,
    [freight_amount_base]                  DECIMAL (26, 3) NULL,
    [import_sequence_number]               INT             NULL,
    [invoice_id]                           VARCHAR (36)    NULL,
    [invoice_number]                       NVARCHAR (100)  NULL,
    [is_price_locked]                      BIT             NULL,
    [is_price_locked_name]                 NVARCHAR (255)  NULL,
    [last_back_office_submit]              DATETIME        NULL,
    [ltf_club_id_name]                     NVARCHAR (100)  NULL,
    [ltf_membership_source]                INT             NULL,
    [ltf_membership_source_name]           NVARCHAR (255)  NULL,
    [modified_by_name]                     NVARCHAR (200)  NULL,
    [modified_by_yomi_name]                NVARCHAR (200)  NULL,
    [modified_on]                          DATETIME        NULL,
    [modified_on_behalf_by_name]           NVARCHAR (200)  NULL,
    [modified_on_behalf_by_yomi_name]      NVARCHAR (200)  NULL,
    [name]                                 NVARCHAR (300)  NULL,
    [opportunity_id_name]                  NVARCHAR (300)  NULL,
    [overridden_created_on]                DATETIME        NULL,
    [owner_id_name]                        NVARCHAR (200)  NULL,
    [owner_id_type]                        NVARCHAR (64)   NULL,
    [owner_id_yomi_name]                   NVARCHAR (200)  NULL,
    [payment_terms_code]                   INT             NULL,
    [payment_terms_code_name]              NVARCHAR (255)  NULL,
    [price_level_id_name]                  NVARCHAR (100)  NULL,
    [pricing_error_code]                   INT             NULL,
    [pricing_error_code_name]              NVARCHAR (255)  NULL,
    [priority_code]                        INT             NULL,
    [priority_code_name]                   NVARCHAR (255)  NULL,
    [sales_order_id_name]                  NVARCHAR (300)  NULL,
    [shipping_method_code]                 INT             NULL,
    [shipping_method_code_name]            NVARCHAR (255)  NULL,
    [ship_to_city]                         NVARCHAR (80)   NULL,
    [ship_to_composite]                    VARCHAR (8000)  NULL,
    [ship_to_country]                      NVARCHAR (80)   NULL,
    [ship_to_fax]                          NVARCHAR (50)   NULL,
    [ship_to_freight_terms_code]           INT             NULL,
    [ship_to_freight_terms_code_name]      NVARCHAR (255)  NULL,
    [ship_to_line_1]                       NVARCHAR (250)  NULL,
    [ship_to_line_2]                       NVARCHAR (250)  NULL,
    [ship_to_line_3]                       NVARCHAR (250)  NULL,
    [ship_to_name]                         NVARCHAR (200)  NULL,
    [ship_to_postal_code]                  NVARCHAR (20)   NULL,
    [ship_to_state_or_province]            NVARCHAR (50)   NULL,
    [ship_to_telephone]                    NVARCHAR (50)   NULL,
    [state_code]                           INT             NULL,
    [state_code_name]                      NVARCHAR (255)  NULL,
    [status_code]                          INT             NULL,
    [status_code_name]                     NVARCHAR (255)  NULL,
    [time_zone_rule_version_number]        INT             NULL,
    [total_amount]                         DECIMAL (26, 3) NULL,
    [total_amount_base]                    DECIMAL (26, 3) NULL,
    [total_amount_less_freight]            DECIMAL (26, 3) NULL,
    [total_amount_less_freight_base]       DECIMAL (26, 3) NULL,
    [total_discount_amount]                DECIMAL (26, 3) NULL,
    [total_discount_amount_base]           DECIMAL (26, 3) NULL,
    [total_line_item_amount]               DECIMAL (26, 3) NULL,
    [total_line_item_amount_base]          DECIMAL (26, 3) NULL,
    [total_line_item_discount_amount]      DECIMAL (26, 3) NULL,
    [total_line_item_discount_amount_base] DECIMAL (26, 3) NULL,
    [total_tax]                            DECIMAL (26, 3) NULL,
    [total_tax_base]                       DECIMAL (26, 3) NULL,
    [transaction_currency_id_name]         NVARCHAR (100)  NULL,
    [utc_conversion_time_zone_code]        INT             NULL,
    [version_number]                       BIGINT          NULL,
    [will_call]                            BIT             NULL,
    [will_call_name]                       NVARCHAR (255)  NULL,
    [inserted_date_time]                   DATETIME        NULL,
    [insert_user]                          VARCHAR (100)   NULL,
    [updated_date_time]                    DATETIME        NULL,
    [update_user]                          VARCHAR (50)    NULL,
    [dv_load_date_time]                    DATETIME        NOT NULL,
    [dv_r_load_source_id]                  BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL,
    [dv_hash]                              CHAR (32)       NOT NULL,
    [dv_batch_id]                          BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([s_crmcloudsync_invoice_id]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_invoice_delete2]([dv_batch_id] ASC);
