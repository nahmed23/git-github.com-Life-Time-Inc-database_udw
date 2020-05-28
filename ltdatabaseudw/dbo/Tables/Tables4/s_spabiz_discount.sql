CREATE TABLE [dbo].[s_spabiz_discount] (
    [s_spabiz_discount_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [discount_id]           DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [discount_delete]       DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (180)   NULL,
    [quick_id]              VARCHAR (45)    NULL,
    [amount]                DECIMAL (26, 6) NULL,
    [pay_retail_comish]     DECIMAL (26, 6) NULL,
    [pay_service_comish]    DECIMAL (26, 6) NULL,
    [pay_comish]            DECIMAL (26, 6) NULL,
    [is_promo]              DECIMAL (26, 6) NULL,
    [use_date_range]        DECIMAL (26, 6) NULL,
    [from_date]             DATETIME        NULL,
    [to_date]               DATETIME        NULL,
    [apply_to]              DECIMAL (26, 6) NULL,
    [discount_filter]       DECIMAL (26, 6) NULL,
    [apply_when]            DECIMAL (26, 6) NULL,
    [percent_discount]      DECIMAL (26, 6) NULL,
    [percent_dollar]        DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [description]           VARCHAR (765)   NULL,
    [descriptiton]          VARCHAR (150)   NULL,
    [security_level]        DECIMAL (26, 6) NULL,
    [one_time]              DECIMAL (26, 6) NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_discount]
    ON [dbo].[s_spabiz_discount]([bk_hash] ASC, [s_spabiz_discount_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_discount]([dv_batch_id] ASC);

