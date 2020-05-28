CREATE TABLE [dbo].[stage_hash_mms_Guest] (
    [stage_hash_mms_Guest_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)    NOT NULL,
    [GuestID]                 INT          NULL,
    [CardNumber]              VARCHAR (25) NULL,
    [FirstName]               VARCHAR (50) NULL,
    [MiddleName]              VARCHAR (50) NULL,
    [LastName]                VARCHAR (50) NULL,
    [AddressLine1]            VARCHAR (50) NULL,
    [AddressLine2]            VARCHAR (50) NULL,
    [City]                    VARCHAR (50) NULL,
    [State]                   VARCHAR (15) NULL,
    [ZIP]                     VARCHAR (11) NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [MaskedPersonalID]        VARCHAR (25) NULL,
    [dv_load_date_time]       DATETIME     NOT NULL,
    [dv_inserted_date_time]   DATETIME     NOT NULL,
    [dv_insert_user]          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]    DATETIME     NULL,
    [dv_update_user]          VARCHAR (50) NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

