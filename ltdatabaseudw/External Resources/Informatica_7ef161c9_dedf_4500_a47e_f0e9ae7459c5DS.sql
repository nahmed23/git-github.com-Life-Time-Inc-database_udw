CREATE EXTERNAL DATA SOURCE [Informatica_7ef161c9_dedf_4500_a47e_f0e9ae7459c5DS]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://udwdata@lttprdudwstorage.blob.core.windows.net/',
    CREDENTIAL = [Informatica_lttprdudwstorage]
    );

