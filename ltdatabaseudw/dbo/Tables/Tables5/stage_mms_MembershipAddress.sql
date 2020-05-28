CREATE TABLE [dbo].[stage_mms_MembershipAddress] (
    [stage_mms_MembershipAddress_id] BIGINT       NOT NULL,
    [MembershipAddressID]            INT          NULL,
    [MembershipID]                   INT          NULL,
    [AddressLine1]                   VARCHAR (50) NULL,
    [AddressLine2]                   VARCHAR (50) NULL,
    [City]                           VARCHAR (50) NULL,
    [ValAddressTypeID]               INT          NULL,
    [Zip]                            VARCHAR (11) NULL,
    [InsertedDateTime]               DATETIME     NULL,
    [ValCountryID]                   INT          NULL,
    [ValStateID]                     INT          NULL,
    [UpdatedDateTime]                DATETIME     NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

