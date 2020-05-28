CREATE TABLE [dbo].[s_ig_it_trn_emp_cash_BD] (
    [s_ig_it_trn_emp_cash_BD_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)       NOT NULL,
    [bus_day_id]                 INT             NULL,
    [cash_drop_amt]              DECIMAL (26, 6) NULL,
    [cashier_emp_id]             INT             NULL,
    [loan_amt]                   DECIMAL (26, 6) NULL,
    [meal_period_id]             INT             NULL,
    [paid_out_amt]               DECIMAL (26, 6) NULL,
    [profit_center_id]           INT             NULL,
    [tender_id]                  INT             NULL,
    [withdrawal_amt]             DECIMAL (26, 6) NULL,
    [dummy_modified_date_time]   DATETIME        NULL,
    [dv_load_date_time]          DATETIME        NOT NULL,
    [dv_r_load_source_id]        BIGINT          NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL,
    [dv_hash]                    CHAR (32)       NOT NULL,
    [dv_deleted]                 BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

