-- validate() repeatedly asks for the oldest pending bronze rows. This index holds only
-- the pending rows, so it stays small however large bronze grows. Owner: Student 1.

CREATE INDEX raw_records_pending_idx
    ON bronze.raw_records (id)
    WHERE validation_status = 'pending';
