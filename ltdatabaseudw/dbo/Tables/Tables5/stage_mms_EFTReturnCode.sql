CREATE TABLE [dbo].[stage_mms_EFTReturnCode] (
    [stage_mms_EFTReturnCode_id] BIGINT       NOT NULL,
    [EFTReturnCodeID]            INT          NULL,
    [ReasonCodeID]               INT          NULL,
    [ValMembershipMessageTypeID] TINYINT      NULL,
    [StopEFTFlag]                BIT          NULL,
    [ReturnCode]                 VARCHAR (10) NULL,
    [Description]                VARCHAR (50) NULL,
    [EFTDeclinedFlag]            BIT          NULL,
    [InsertedDateTime]           DATETIME     NULL,
    [EFTChargeBackFlag]          BIT          NULL,
    [UpdatedDateTime]            DATETIME     NULL,
    [dv_batch_id]                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

