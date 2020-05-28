CREATE VIEW [marketing].[v_fact_affinitech_accuracy_audit]
AS select fact_affinitech_accuracy_audit.Accuracy Accuracy,
       fact_affinitech_accuracy_audit.Count Count,
       fact_affinitech_accuracy_audit.date date,
       fact_affinitech_accuracy_audit.fact_affinitech_accuracy_audit_key fact_affinitech_accuracy_audit_key,
       fact_affinitech_accuracy_audit.studio studio,
       fact_affinitech_accuracy_audit.transactions transactions
  from dbo.fact_affinitech_accuracy_audit;