CREATE PROC [sandbox_ebi].[msftPoC_elt_tableA] @p_batch_id [INT] AS
BEGIN
set nocount on

create table #stageTable with (HEAP, distribution=hash(ID)) as
select a.ID, a.ShortDesc, a.NumVal, a.batch_id, getdate() as di_LastModifiedDateTime
from sandbox_ebi.msftPoC_stage_tableA a
where a.batch_id = @p_batch_id

UPDATE sandbox_ebi.msftPoC_tableA
SET ShortDesc = stg.ShortDesc
	, NumVal = stg.NumVal
	, batch_id = stg.batch_id
	, di_LastModifiedDateTime = stg.di_LastModifiedDateTime
FROM #stageTable stg
where stg.ID=sandbox_ebi.msftPoC_tableA.ID

INSERT INTO sandbox_ebi.msftPoC_tableA
select stg.*
from #stageTable stg
	left join sandbox_ebi.msftPoC_tableA tgt
		on tgt.ID = stg.ID
where tgt.ID is null
END
