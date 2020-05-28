CREATE EXTERNAL TABLE [dbo].[Informatica_805266aa_b39c_4634_b9e4_83df480f214aExt] (
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
    DATA_SOURCE = [Informatica_805266aa_b39c_4634_b9e4_83df480f214aDS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_805266aa_b39c_4634_b9e4_83df480f214a',
    FILE_FORMAT = [Informatica_805266aa_b39c_4634_b9e4_83df480f214aFF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

