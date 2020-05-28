CREATE TABLE [dbo].[stage_mms_Guest] (
    [stage_mms_Guest_id] BIGINT       NOT NULL,
    [GuestID]            INT          NULL,
    [CardNumber]         VARCHAR (25) NULL,
    [FirstName]          VARCHAR (50) NULL,
    [MiddleName]         VARCHAR (50) NULL,
    [LastName]           VARCHAR (50) NULL,
    [AddressLine1]       VARCHAR (50) NULL,
    [AddressLine2]       VARCHAR (50) NULL,
    [City]               VARCHAR (50) NULL,
    [State]              VARCHAR (15) NULL,
    [ZIP]                VARCHAR (11) NULL,
    [InsertedDateTime]   DATETIME     NULL,
    [UpdatedDateTime]    DATETIME     NULL,
    [MaskedPersonalID]   VARCHAR (25) NULL,
    [dv_batch_id]        BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

