# Data Quality Notes

## etl_runs validation_status discontinuity

Prior to commit 6e0f64d (fix: align validation_status string), `load_raw.py` wrote `'in_progress'` as the initial `validation_status`, but `validate_raw.py` queried `WHERE validation_status = 'pending'` — a string mismatch that caused validation to silently never find any runs to process.

As of this fix, `validate_raw.py` correctly queries `WHERE validation_status = 'in_progress'`.

**Historical rows affected:**
- 2 rows remain stuck at `validation_status = 'in_progress'` — these were loaded but never validated. They should be re-validated manually if audit completeness matters.
- 5 rows remain at `validation_status = 'pending'` — likely from an earlier code version before `load_raw.py` was changed to write `'in_progress'`. These were never validated either.

These rows have not been modified or deleted; they are documented here for transparency.
