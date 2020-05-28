CREATE TABLE [dbo].[stage_mms_ClubAddress] (
    [stage_mms_ClubAddress_id] BIGINT          NOT NULL,
    [ClubAddressID]            INT             NULL,
    [ClubID]                   INT             NULL,
    [AddressLine1]             VARCHAR (50)    NULL,
    [AddressLine2]             VARCHAR (50)    NULL,
    [City]                     VARCHAR (50)    NULL,
    [ValAddressTypeID]         INT             NULL,
    [Zip]                      VARCHAR (11)    NULL,
    [InsertedDateTime]         DATETIME        NULL,
    [ValCountryID]             INT             NULL,
    [ValStateID]               INT             NULL,
    [UpdatedDateTime]          DATETIME        NULL,
    [Latitude]                 DECIMAL (20, 9) NULL,
    [Longitude]                DECIMAL (20, 9) NULL,
    [MapCenterLatitude]        DECIMAL (10, 4) NULL,
    [MapCenterLongitude]       DECIMAL (10, 4) NULL,
    [MapZoomLevel]             INT             NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

