CREATE TABLE [dbo].[map_ltfeb_party_id_dim_employee_key] (
    [map_ltfeb_party_id_dim_employee_key_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [assigned_id]                            VARCHAR (25) NULL,
    [dim_employee_key]                       VARCHAR (32) NULL,
    [party_id]                               INT          NULL,
    [dv_load_date_time]                      DATETIME     NULL,
    [dv_load_end_date_time]                  DATETIME     NULL,
    [dv_batch_id]                            BIGINT       NOT NULL,
    [dv_inserted_date_time]                  DATETIME     NOT NULL,
    [dv_insert_user]                         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                   DATETIME     NULL,
    [dv_update_user]                         VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

