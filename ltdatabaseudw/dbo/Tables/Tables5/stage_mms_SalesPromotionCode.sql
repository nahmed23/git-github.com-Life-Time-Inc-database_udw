CREATE TABLE [dbo].[stage_mms_SalesPromotionCode] (
    [stage_mms_SalesPromotionCode_id] BIGINT        NOT NULL,
    [SalesPromotionCodeID]            INT           NULL,
    [SalesPromotionID]                INT           NULL,
    [MemberID]                        INT           NULL,
    [PromotionCode]                   VARCHAR (50)  NULL,
    [ExpirationDate]                  DATETIME      NULL,
    [UsageLimit]                      SMALLINT      NULL,
    [NotifyEmailAddress]              VARCHAR (140) NULL,
    [NumberOfCodeRecipients]          SMALLINT      NULL,
    [InsertedDateTime]                DATETIME      NULL,
    [UpdatedDateTime]                 DATETIME      NULL,
    [DisplayUIFlag]                   BIT           NULL,
    [dv_batch_id]                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

