CREATE TABLE [dbo].[stage_FactGoal_NBOB] (
    [ClubID]      INT             NULL,
    [ClubCode]    NVARCHAR (10)   NULL,
    [Description] NVARCHAR (255)  NULL,
    [Month_Year]  DATETIME        NULL,
    [Budget]      DECIMAL (26, 6) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

