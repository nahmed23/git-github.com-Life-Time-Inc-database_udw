CREATE EXTERNAL DATA SOURCE [Informatica_41d555fc_fe52_4489_accf_b3c9276e10eeDS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

