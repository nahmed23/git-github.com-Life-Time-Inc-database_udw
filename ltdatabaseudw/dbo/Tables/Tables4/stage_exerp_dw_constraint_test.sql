CREATE TABLE [dbo].[stage_exerp_dw_constraint_test] (
    [stage_exerp_dw_constraint_test_id] BIGINT         NOT NULL,
    [test_number]                       VARCHAR (4000) NULL,
    [table_1]                           VARCHAR (4000) NULL,
    [table_2]                           VARCHAR (4000) NULL,
    [foreign_key]                       VARCHAR (4000) NULL,
    [primary_key]                       VARCHAR (4000) NULL,
    [nullable]                          BIT            NULL,
    [relationship]                      VARCHAR (4000) NULL,
    [extra_con]                         VARCHAR (4000) NULL,
    [test_query]                        VARCHAR (4000) NULL,
    [dummy_modified_date_time]          DATETIME       NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

