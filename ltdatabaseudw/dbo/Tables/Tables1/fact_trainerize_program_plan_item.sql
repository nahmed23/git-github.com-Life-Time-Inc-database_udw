CREATE TABLE [dbo].[fact_trainerize_program_plan_item] (
    [fact_trainerize_program_plan_item_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [completed_flag]                        CHAR (1)        NULL,
    [created_dim_date_key]                  VARCHAR (8)     NULL,
    [dim_mms_member_key]                    VARCHAR (32)    NULL,
    [dim_trainerize_plan_key]               VARCHAR (32)    NULL,
    [dim_trainerize_program_key]            VARCHAR (32)    NULL,
    [fact_trainerize_program_plan_item_key] VARCHAR (32)    NULL,
    [item_description]                      NVARCHAR (4000) NULL,
    [item_dim_date_key]                     VARCHAR (8)     NULL,
    [item_dim_time_key]                     INT             NULL,
    [item_name]                             NVARCHAR (4000) NULL,
    [item_type]                             INT             NULL,
    [plan_dim_employee_key]                 VARCHAR (32)    NULL,
    [plan_item_id]                          INT             NULL,
    [program_dim_employee_key]              VARCHAR (32)    NULL,
    [source_id]                             NVARCHAR (50)   NULL,
    [source_type]                           INT             NULL,
    [updated_dim_date_key]                  VARCHAR (8)     NULL,
    [dv_load_date_time]                     DATETIME        NULL,
    [dv_load_end_date_time]                 DATETIME        NULL,
    [dv_batch_id]                           BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_trainerize_program_plan_item_key]));

