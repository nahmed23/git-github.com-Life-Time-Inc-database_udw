CREATE EXTERNAL TABLE [dbo].[Informatica_41d555fc_fe52_4489_accf_b3c9276e10eeExt] (
    [stage_lt_bucks_Transactions_id] BIGINT NULL,
    [transaction_id] INT NULL,
    [transaction_type] INT NULL,
    [transaction_user] INT NULL,
    [transaction_amount] DECIMAL (26, 6) NULL,
    [transaction_session] INT NULL,
    [transaction_ext_ref] NVARCHAR (150) NULL,
    [transaction_int1] INT NULL,
    [transaction_int2] INT NULL,
    [transaction_date1] DATETIME NULL,
    [transaction_timestamp] DATETIME NULL,
    [transaction_promotion] INT NULL,
    [transaction_int3] INT NULL,
    [transaction_int4] INT NULL,
    [transaction_int5] INT NULL,
    [LastModifiedTimestamp] DATETIME NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_41d555fc_fe52_4489_accf_b3c9276e10eeDS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_41d555fc_fe52_4489_accf_b3c9276e10ee',
    FILE_FORMAT = [Informatica_41d555fc_fe52_4489_accf_b3c9276e10eeFF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

