-----------------------------------------------------------------------------
-- Auxiliar database to avoid dupliacte observations into the ACTION Database
-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zooniverse_t
(

	classification_id	INTEGER,
	workflow_id			INTEGER,	
	workflow_name		TEXT,	
	workflow_version	TEXT,
	created_at			TEXT,	
	gold_standard		TEXT,	
	expert				TEXT,	
	metadata			TEXT,	-- long JSON string with netsted info
	annotations			TEXT,	-- long JSON string with netsted info
	subject_data		TEXT,	-- long JSON string with netsted info
	subject_ids			TEXT,	-- JSON string

	PRIMARY KEY(classification_id)
);
