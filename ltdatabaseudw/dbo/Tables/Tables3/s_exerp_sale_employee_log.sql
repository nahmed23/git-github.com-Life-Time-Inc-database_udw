CREATE TABLE [dbo].[s_exerp_sale_employee_log] (
    [s_exerp_sale_employee_log_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [sale_employee_log_id]         INT          NULL,
    [from_datetime]                DATETIME     NULL,
    [ets]                          BIGINT       NULL,
    [dummy_modified_date_time]     DATETIME     NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_hash]                      CHAR (32)    NOT NULL,
    [dv_deleted]                   BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

