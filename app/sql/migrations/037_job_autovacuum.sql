ALTER TABLE
  job
SET
  (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01
  );
