CREATE TABLE [dbo].[stage_hash_mdm_GoldenRecordCustomerPhone] (
    [stage_hash_mdm_GoldenRecordCustomerPhone_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)      NOT NULL,
    [PhoneType]                                   CHAR (40)      NULL,
    [Phone]                                       CHAR (40)      NULL,
    [LoadDateTime]                                DATETIME2 (7)  NULL,
    [EntityID]                                    NVARCHAR (128) NULL,
    [InsertedDateTime]                            DATETIME2 (7)  NULL,
    [dv_load_date_time]                           DATETIME       NOT NULL,
    [dv_inserted_date_time]                       DATETIME       NOT NULL,
    [dv_insert_user]                              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                        DATETIME       NULL,
    [dv_update_user]                              VARCHAR (50)   NULL,
    [dv_batch_id]                                 BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

