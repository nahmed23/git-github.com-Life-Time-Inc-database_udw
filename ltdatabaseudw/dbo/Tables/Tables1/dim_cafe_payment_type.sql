CREATE TABLE [dbo].[dim_cafe_payment_type] (
    [dim_cafe_payment_type_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_cafe_payment_type_key] CHAR (32)     NULL,
    [payment_class]             NVARCHAR (50) NULL,
    [payment_type]              NVARCHAR (50) NULL,
    [tender_id]                 INT           NULL,
    [dv_load_date_time]         DATETIME      NULL,
    [dv_load_end_date_time]     DATETIME      NULL,
    [dv_batch_id]               BIGINT        NOT NULL,
    [dv_inserted_date_time]     DATETIME      NOT NULL,
    [dv_insert_user]            VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]      DATETIME      NULL,
    [dv_update_user]            VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_cafe_payment_type_key]));

