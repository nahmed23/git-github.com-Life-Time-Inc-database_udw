CREATE TABLE [dbo].[MMSMembershipAddress] (
    [MMSMembershipAddressKey] INT          NOT NULL,
    [MembershipAddressID]     INT          NOT NULL,
    [MembershipID]            INT          NULL,
    [AddressLine1]            VARCHAR (50) NULL,
    [AddressLine2]            VARCHAR (50) NULL,
    [City]                    VARCHAR (50) NULL,
    [ValAddressTypeID]        TINYINT      NULL,
    [Zip]                     VARCHAR (11) NULL,
    [MMSInsertedDateTime]     DATETIME     NULL,
    [ValCountryID]            TINYINT      NULL,
    [ValStateID]              SMALLINT     NULL,
    [MMSUpdatedDateTime]      DATETIME     NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [InsertUser]              VARCHAR (50) NULL,
    [BatchID]                 INT          NOT NULL,
    [ETLSourceSystemKey]      INT          NOT NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [UpdateUser]              VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([MembershipID]));

