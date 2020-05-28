CREATE TABLE [dbo].[dim_client_location] (
    [dim_client_location_key] BIGINT        IDENTITY (1, 1) NOT NULL,
    [Parent_Client]           VARCHAR (100) NULL,
    [Parent_ReportingGroupID] VARCHAR (30)  NULL,
    [LocationID]              VARCHAR (50)  NULL,
    [SubClient_Description]   VARCHAR (100) NULL,
    [Parent_OR_Child]         VARCHAR (100) NULL,
    [Address1]                VARCHAR (200) NULL,
    [Address2]                VARCHAR (200) NULL,
    [City]                    VARCHAR (50)  NULL,
    [State]                   VARCHAR (30)  NULL,
    [Zip]                     VARCHAR (10)  NULL,
    [Country]                 VARCHAR (20)  NULL,
    [Phone]                   VARCHAR (15)  NULL,
    [Email]                   VARCHAR (50)  NULL,
    [store_number]            INT           NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

