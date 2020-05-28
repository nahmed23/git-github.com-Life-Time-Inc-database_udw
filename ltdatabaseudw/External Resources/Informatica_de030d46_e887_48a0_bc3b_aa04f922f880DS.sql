CREATE EXTERNAL DATA SOURCE [Informatica_de030d46_e887_48a0_bc3b_aa04f922f880DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

