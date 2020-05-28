CREATE TABLE [dbo].[Temp_FactNewMembership] (
    [FactNewMembershipKey]                          INT             NOT NULL,
    [DimLocationKey]                                INT             NOT NULL,
    [MMSClubID]                                     INT             NULL,
    [EmployeeID]                                    INT             NULL,
    [MemberID]                                      INT             NULL,
    [PrimarySalesDimEmployeeKey]                    INT             NOT NULL,
    [DimCustomerKey]                                INT             NOT NULL,
    [MembershipID]                                  INT             NOT NULL,
    [EnrollmentFee]                                 NUMERIC (12, 2) NOT NULL,
    [InsertedDateTime]                              DATETIME        NOT NULL,
    [FactMembershipKey]                             INT             NOT NULL,
    [CorporateMembershipFlag]                       CHAR (1)        NOT NULL,
    [IncludeInDSSRFlag]                             CHAR (1)        NOT NULL,
    [OriginalCurrencyCode]                          VARCHAR (15)    NOT NULL,
    [USDMonthlyAverageDimExchangeRateKey]           INT             NOT NULL,
    [USDDimPlanExchangeRateKey]                     INT             NOT NULL,
    [LocalCurrencyMonthlyAverageDimExchangeRateKey] INT             NOT NULL,
    [LocalCurrencyDimPlanExchangeRateKey]           INT             NOT NULL,
    [MembershipTypeID]                              INT             NULL,
    [CreatedDateTime]                               DATETIME        NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

