-----------------------------------------------------------------------------
-- Auxiliar database to avoid duplicate observations into the ACTION Database
-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zooniverse_export_t
(
	classification_id	INTEGER,
	user_name           TEXT, 	-- only for registered users
	user_id             TEXT,	-- only for registered users
	workflow_id			INTEGER,	
	workflow_name		TEXT,	
	workflow_version	TEXT,
	created_at			TEXT,	
	gold_standard		TEXT,	
	expert				TEXT,	
	metadata			TEXT,	-- long JSON string with nested info
	annotations			TEXT,	-- long JSON string with nested info
	subject_data		TEXT,	-- long JSON string with nested info
	subject_ids			TEXT,	-- JSON string

	PRIMARY KEY(classification_id)
);
