CREATE TABLE [dbo].[stage_mms_EFTAccount] (
    [stage_mms_EFTAccount_id] BIGINT   NOT NULL,
    [EFTAccountID]            INT      NULL,
    [BankAccountID]           INT      NULL,
    [MembershipID]            INT      NULL,
    [CreditCardAccountID]     INT      NULL,
    [BankAccountFlag]         BIT      NULL,
    [InsertedDateTime]        DATETIME NULL,
    [UpdatedDateTime]         DATETIME NULL,
    [dv_batch_id]             BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

