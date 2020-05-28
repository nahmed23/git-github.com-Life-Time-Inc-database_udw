﻿CREATE TABLE [dbo].[stage_hash_ig_it_trn_Tender_Cum_BD] (
    [stage_hash_ig_it_trn_Tender_Cum_BD_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [bus_day_id]                            INT             NULL,
    [check_type_id]                         INT             NULL,
    [meal_period_id]                        INT             NULL,
    [cashier_emp_id]                        INT             NULL,
    [PMS_post_code]                         SMALLINT        NULL,
    [profit_center_id]                      INT             NULL,
    [tax_removed_code]                      SMALLINT        NULL,
    [tender_id]                             INT             NULL,
    [void_type_id]                          INT             NULL,
    [base_tender_amt]                       DECIMAL (26, 6) NULL,
    [breakage_amt]                          DECIMAL (26, 6) NULL,
    [change_amt]                            DECIMAL (26, 6) NULL,
    [received_curr_amt]                     DECIMAL (26, 6) NULL,
    [tender_qty]                            INT             NULL,
    [tip_amt]                               DECIMAL (26, 6) NULL,
    [dummy_modified_date_time]              DATETIME        NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_batch_id]                           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

