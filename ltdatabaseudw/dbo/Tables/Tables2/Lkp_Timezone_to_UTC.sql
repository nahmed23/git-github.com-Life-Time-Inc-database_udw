CREATE TABLE [dbo].[Lkp_Timezone_to_UTC] (
    [Location]   VARCHAR (100) NULL,
    [Hours_Diff] INT           NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

