﻿CREATE TABLE [dbo].[stage_hash_ig_it_trn_Emp_Cash_BD] (
    [stage_hash_ig_it_trn_Emp_Cash_BD_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [bus_day_id]                          INT             NULL,
    [cash_drop_amt]                       DECIMAL (26, 6) NULL,
    [cashier_emp_id]                      INT             NULL,
    [loan_amt]                            DECIMAL (26, 6) NULL,
    [meal_period_id]                      INT             NULL,
    [paidout_amt]                         DECIMAL (26, 6) NULL,
    [profit_center_id]                    INT             NULL,
    [tender_id]                           INT             NULL,
    [withdrawal_amt]                      DECIMAL (26, 6) NULL,
    [BD_start_dttime]                     DATETIME        NULL,
    [BD_end_dttime]                       DATETIME        NULL,
    [dummy_modified_date_time]            DATETIME        NULL,
    [dv_load_date_time]                   DATETIME        NOT NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

