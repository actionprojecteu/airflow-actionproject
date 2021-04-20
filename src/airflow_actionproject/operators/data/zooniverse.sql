-----------------------------------------------------------------------------
-- Auxiliar database to avoid duplicate observations into the ACTION Database
-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zooniverse_export_t
(
    classification_id   INTEGER,
    user_name           TEXT,   -- only for registered users
    user_id             TEXT,   -- only for registered users
    user_ip             TEXT,
    workflow_id         INTEGER,    
    workflow_name       TEXT,   
    workflow_version    TEXT,
    created_at          TEXT,   
    gold_standard       TEXT,   -- JSON string  
    expert              TEXT,   -- JSON string
    metadata            TEXT,   -- long JSON string with nested info
    annotations         TEXT,   -- long JSON string with nested info
    subject_data        TEXT,   -- long JSON string with nested info
    subject_ids         TEXT,   -- JSON string

    PRIMARY KEY(classification_id)
);

----------------------------------------------------------------------
-- This table keeps track of classification runs
-- If we run a classification by Airflow backfilling
-- we may loose track of a window of classifications not dumped to the
-- ACTION database.
-- This history log helps identify when and provides info to fix it.
---------------------------------------------------------------------- 

CREATE TABLE IF NOT EXISTS zooniverse_window_t
(
    executed_at         TEXT,   -- execution timestamp
    before              TEXT,   -- lastest classification timestamp before insertion
    after               TEXT,   -- lastest classification timestamp after insertion
    PRIMARY KEY(executed_at)
);
