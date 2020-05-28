﻿CREATE TABLE [dbo].[dim_spabiz_service] (
    [dim_spabiz_service_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_spabiz_service_key]      CHAR (32)       NULL,
    [service_id]                  BIGINT          NULL,
    [store_number]                BIGINT          NULL,
    [book_name]                   VARCHAR (150)   NULL,
    [call_after_x_days]           DECIMAL (26, 6) NULL,
    [category]                    VARCHAR (50)    NULL,
    [color_balance_flag]          CHAR (1)        NULL,
    [commission_mapping]          VARCHAR (25)    NULL,
    [cost]                        DECIMAL (26, 6) NULL,
    [cost_type]                   DECIMAL (26, 6) NULL,
    [created_date_time]           DATETIME        NULL,
    [deleted_date_time]           DATETIME        NULL,
    [deleted_flag]                CHAR (1)        NULL,
    [dim_spabiz_category_key]     CHAR (32)       NULL,
    [dim_spabiz_store_key]        CHAR (32)       NULL,
    [edit_date_time]              DATETIME        NULL,
    [finish]                      VARCHAR (15)    NULL,
    [fixed_currency_amount_flag]  CHAR (1)        NULL,
    [gl_account]                  VARCHAR (45)    NULL,
    [highlight_procedure_flag]    CHAR (1)        NULL,
    [level_1_service_category]    VARCHAR (150)   NULL,
    [level_2_service_category]    VARCHAR (150)   NULL,
    [level_3_service_category]    VARCHAR (150)   NULL,
    [new_customer_extra_time]     VARCHAR (30)    NULL,
    [pay_commission_flag]         CHAR (1)        NULL,
    [percent_of_total_price_flag] CHAR (1)        NULL,
    [quick_id]                    VARCHAR (90)    NULL,
    [require_staff_flag]          CHAR (1)        NULL,
    [resource_count]              DECIMAL (26, 6) NULL,
    [retail_price]                DECIMAL (26, 6) NULL,
    [segment]                     VARCHAR (50)    NULL,
    [service_description]         VARCHAR (3000)  NULL,
    [service_level]               DECIMAL (26, 6) NULL,
    [service_name]                VARCHAR (150)   NULL,
    [service_process]             VARCHAR (15)    NULL,
    [service_time]                VARCHAR (15)    NULL,
    [spabiz_category]             VARCHAR (150)   NULL,
    [taxable_flag]                CHAR (1)        NULL,
    [web_book_flag]               CHAR (1)        NULL,
    [web_view_flag]               CHAR (1)        NULL,
    [p_spabiz_service_id]         BIGINT          NULL,
    [dv_load_date_time]           DATETIME        NULL,
    [dv_load_end_date_time]       DATETIME        NULL,
    [dv_batch_id]                 BIGINT          NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_spabiz_service_key]));
