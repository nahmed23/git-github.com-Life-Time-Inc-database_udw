CREATE TABLE [dbo].[d_mms_employee_role] (
    [d_mms_employee_role_id]                             BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                            CHAR (32)    NOT NULL,
    [employee_role_id]                                   INT          NULL,
    [assistant_department_head_sales_for_net_units_flag] CHAR (1)     NULL,
    [department_head_sales_for_net_units_flag]           CHAR (1)     NULL,
    [dim_employee_key]                                   CHAR (32)    NULL,
    [dim_employee_role_key]                              VARCHAR (32) NULL,
    [employee_id]                                        INT          NULL,
    [primary_employee_role_flag]                         CHAR (1)     NULL,
    [sales_group_flag]                                   CHAR (1)     NULL,
    [sales_manager_flag]                                 CHAR (1)     NULL,
    [p_mms_employee_role_id]                             BIGINT       NOT NULL,
    [deleted_flag]                                       INT          NULL,
    [dv_load_date_time]                                  DATETIME     NULL,
    [dv_load_end_date_time]                              DATETIME     NULL,
    [dv_batch_id]                                        BIGINT       NOT NULL,
    [dv_inserted_date_time]                              DATETIME     NOT NULL,
    [dv_insert_user]                                     VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                               DATETIME     NULL,
    [dv_update_user]                                     VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_employee_role]([dv_batch_id] ASC);

