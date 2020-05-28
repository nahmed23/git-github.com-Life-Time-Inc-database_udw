CREATE TABLE [dbo].[l_mms_product] (
    [l_mms_product_id]                      BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [product_id]                            INT          NULL,
    [department_id]                         INT          NULL,
    [gl_over_ride_club_id]                  INT          NULL,
    [val_gl_group_id]                       INT          NULL,
    [val_recurrent_product_type_id]         INT          NULL,
    [val_product_status_id]                 INT          NULL,
    [val_assessment_day_id]                 INT          NULL,
    [workday_account]                       VARCHAR (6)  NULL,
    [workday_cost_center]                   VARCHAR (6)  NULL,
    [workday_offering]                      VARCHAR (10) NULL,
    [workday_over_ride_region]              VARCHAR (4)  NULL,
    [workday_revenue_product_group_account] VARCHAR (6)  NULL,
    [revenue_category]                      VARCHAR (7)  NULL,
    [spend_category]                        VARCHAR (7)  NULL,
    [pay_component]                         VARCHAR (50) NULL,
    [val_employee_level_type_id]            INT          NULL,
    [dv_load_date_time]                     DATETIME     NOT NULL,
    [dv_r_load_source_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL,
    [dv_hash]                               CHAR (32)    NOT NULL,
    [dv_batch_id]                           BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_product]([dv_batch_id] ASC);

