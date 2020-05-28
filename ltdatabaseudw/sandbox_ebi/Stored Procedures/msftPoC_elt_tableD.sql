CREATE PROC [sandbox_ebi].[msftPoC_elt_tableD] @p_batch_id [INT] AS
BEGIN
create table #stageTableD with (HEAP, distribution=hash(ID)) as
select a.ID, a.ShortDesc, a.NumVal + b.NumVal as CalcVal, a.batch_id, getdate() as di_LastModifiedDateTime
from sandbox_ebi.msftPoC_stage_tableA a
	join sandbox_ebi.msftPoC_stage_tableB b
		on a.id=b.id
			and a.batch_id = b.batch_id
where a.batch_id = @p_batch_id

UPDATE sandbox_ebi.msftPoC_tableD
SET ShortDesc = stg.ShortDesc
	, CalcVal = stg.CalcVal
	, batch_id = stg.batch_id
	, di_LastModifiedDateTime = stg.di_LastModifiedDateTime
FROM #stageTableD stg
where stg.ID=sandbox_ebi.msftPoC_tableD.ID

INSERT INTO sandbox_ebi.msftPoC_tableD
select stg.*
from #stageTableD stg
	left join sandbox_ebi.msftPoC_tableD tgt
		on tgt.ID = stg.ID
where tgt.ID is null
END