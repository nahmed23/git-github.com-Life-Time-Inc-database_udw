﻿CREATE EXTERNAL FILE FORMAT [Informatica_de030d46_e887_48a0_bc3b_aa04f922f880FF]
    WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR = N'0x1e', STRING_DELIMITER = N'0x1f', FIRST_ROW = 1, ENCODING = N'UTF8')
    );

