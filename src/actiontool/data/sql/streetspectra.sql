-----------------------------------------------------------------------------
-- Auxiliar database to avoid duplicate observations into the ACTION Database
-----------------------------------------------------------------------------

------------------------------------------------------------------------
-- This is the current Zooniverse export data format
-- Some columns are complex like metadata, annotations and subject_data
-- which containes nested ifnromation
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zooniverse_export_t
(
    classification_id   INTEGER,
    user_name           TEXT,    -- only for registered users
    user_id             INTEGER, -- only for registered users
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

CREATE TABLE IF NOT EXISTS zooniverse_export_window_t
(
    executed_at         TEXT,   -- execution timestamp
    before              TEXT,   -- lastest classification timestamp before insertion
    after               TEXT,   -- lastest classification timestamp after insertion
    PRIMARY KEY(executed_at)
);

------------------------------------------------------------------------
-- This is the table where we extratd all relevant data 
-- from Zooniverse individual classifications entries in the export file
-- in order to make final aggregate classifications
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zooniverse_classification_t
(
    id                  INT,    -- unique Zooinverse classification identifier
    subject_id          INT,    -- Zooinverse image id subbject of classification
    user_id             INT,    -- Zooinverse user id in case of non anonymous classifications
    width               INT,    -- image width
    height              INT,    -- image height
    source_x            INT,    -- light source x coordinate within the image
    source_y            INT,    -- light source y coordinate within the image
    spectrum_x          INT,    -- spectrum box corner point, x coordinate
    spectrum_y          INT,    -- spectrum box corner point, y coordinate
    spectrum_width      INT,    -- spectrum box width
    spectrum_height     INT,    -- spectrum box height
    spectrum_angle      INT,    -- spectrum box angle (degrees) (respect to X?)
    spectrum_type       TEXT,   -- spectrum type (LED, MV, HPS, etc)
    image_id            INT,    -- observing platform image Id
    image_url           TEXT,   -- observing platform image URL
    image_long          REAL,   -- image aprox. longitude
    image_lat           REAL,   -- image aprox. latitude
    image_observer      TEXT,   -- observer nickname if any
    image_comment       TEXT,   -- image optional comment
    image_source        TEXT,   -- observing platform (currently "Epicollect 5")
    image_created_at    TEXT,   -- Image creation UTC timestamp in iso 8601 format, with trailing Z

    PRIMARY KEY(id)
);
