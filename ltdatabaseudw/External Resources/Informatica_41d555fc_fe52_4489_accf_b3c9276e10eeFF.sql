﻿CREATE EXTERNAL FILE FORMAT [Informatica_41d555fc_fe52_4489_accf_b3c9276e10eeFF]
    WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR = N'0x1e', STRING_DELIMITER = N'0x1f', FIRST_ROW = 1, ENCODING = N'UTF8')
    );
