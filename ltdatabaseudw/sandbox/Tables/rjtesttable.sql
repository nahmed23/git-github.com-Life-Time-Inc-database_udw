﻿CREATE TABLE [sandbox].[rjtesttable] (
    [col1] INT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

