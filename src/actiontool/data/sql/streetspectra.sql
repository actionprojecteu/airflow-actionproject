-----------------------------------------------------------------------------
-- Auxiliar database to avoid duplicate observations into the ACTION Database
-----------------------------------------------------------------------------

------------------------------------------------------------------------
-- This is the current Zooniverse export data format
-- This table is generic for all Zooniverse projects
-- Some columns are complex like metadata, annotations and subject_data
-- which containes nested information
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zoo_export_t
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
    metadata            TEXT,   -- long JSON string with deep nested info
    annotations         TEXT,   -- long JSON string with deep nested info
    subject_data        TEXT,   -- long JSON string with deep nested info
    subject_ids         TEXT,   -- JSON string

    PRIMARY KEY(classification_id)
);

----------------------------------------------------------------------
-- This table keeps track of Zooniverse export runs
-- If we run a classification by Airflow backfilling
-- we may loose track of a window of classifications not dumped to the
-- ACTION database.
-- This history log helps identify when and provides info to fix it.
---------------------------------------------------------------------- 

CREATE TABLE IF NOT EXISTS zoo_export_window_t
(
    executed_at         TEXT,   -- execution timestamp
    before              TEXT,   -- lastest classification timestamp before insertion
    after               TEXT,   -- lastest classification timestamp after insertion
    PRIMARY KEY(executed_at)
);

------------------------------------------------------------------------
-- This is the table where we extract all StreetSpectra relevant data 
-- from Zooniverse individual classifications entries in the export file
-- in order to make final aggregate classifications
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS spectra_classification_t
(
    classification_id   INT,    -- unique Zooinverse classification identifier
    subject_id          INT,    -- Zooinverse image id subject of classification
    started_at          TEXT,   -- Classification UTC start timestamp, IS08601 format
    finished_at         TEXT,   -- Classification UTC end timestamp, IS08601 format
    width               INT,    -- image width
    height              INT,    -- image height
    source_id           INT,    -- light source identifier pointed to by user within the subject. Initially NULL
    source_x            REAL,   -- light source x coordinate within the image
    source_y            REAL,   -- light source y coordinate within the image
    spectrum_x          REAL,   -- spectrum box corner point, x coordinate
    spectrum_y          REAL,   -- spectrum box corner point, y coordinate
    spectrum_width      REAL,   -- spectrum box width
    spectrum_height     REAL,   -- spectrum box height
    spectrum_angle      REAL,   -- spectrum box angle (degrees) (respect to X axis?)
    spectrum_type       TEXT,   -- spectrum type ('HPS','MV','LED','MH')
    image_id            INT,    -- observing platform image Id
    image_url           TEXT,   -- observing platform image URL
    image_long          REAL,   -- image aprox. longitude
    image_lat           REAL,   -- image aprox. latitude
    image_observer      TEXT,   -- observer nickname, if any
    image_comment       TEXT,   -- image optional comment
    image_source        TEXT,   -- observing platform name (currently "Epicollect 5")
    image_created_at    TEXT,   -- image creation UTC timestamp, ISO8601 format
    image_spectrum      TEXT,   -- spectrum type, if any, given by observer to his intended target (which we really don't know)

    PRIMARY KEY(classification_id)
);


------------------------------------------------------------------------
-- This is the table where we store all StreetSpectra aggregate
-- classifications data ready to be exported to a suitable file format
-- to Zenodo 
------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS spectra_aggregate_t
(
    subject_id          INT,    -- Zooinverse image id subject of classification
    source_id           INT,    -- light source identifier pointed to by user within the subject.
    width               INT,    -- image width
    height              INT,    -- image height
    source_x            REAL,   -- average light source x coordinate within the image
    source_y            REAL,   -- average light source y coordinate within the image
    spectrum_type       TEXT,   -- spectrum type mode (statistics), One of (LED, MV, HPS, LPS, MH, None) or 'Ambiguous' if such mode do not exists
    spectrum_count      INT,    -- max. classification count for this particular light source
    spectrum_users      INT,    -- number of users for this light source. Percent agreement is (spectrum_count/spectrum_users)
    spectrum_dist       TEXT,   -- classification distribution made by the users for a given light source
    kappa               REAL,   -- Fleiss' Kappa when classifying all source_ids within a given subject_id
    users_count         INT,    -- Number of users that has classified light sources in a given subject_id (used to compute Fleiss' Kappa)
    rejection_tag       TEXT,   -- When spectrum_type is NULL, shows the reason why
    image_id            INT,    -- observing platform image Id
    image_url           TEXT,   -- observing platform image URL
    image_long          REAL,   -- image aprox. longitude
    image_lat           REAL,   -- image aprox. latitude
    image_observer      TEXT,   -- observer nickname, if any
    image_comment       TEXT,   -- image optional comment
    image_source        TEXT,   -- observing platform name (currently "Epicollect 5")
    image_created_at    TEXT,   -- image creation UTC timestamp in iso 8601 format, with trailing Z
    image_spectrum      TEXT,   -- spectrum type, if any, given by observer to his intended target (which we really don't know)

    PRIMARY KEY(subject_id, source_id)
);
