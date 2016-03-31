CREATE TABLE aixm_feature (
  uuid text,
  type text NOT NULL,
  gml_identifier text,
  aixm_feature_metadata bytea,
  CONSTRAINT aixm_feature_pkey PRIMARY KEY (uuid)
);
SELECT ADDGEOMETRYCOLUMN('', 'aixm_feature', 'gml_bounded_by', 0, 'GEOMETRY', 2);
CREATE TABLE aixm_timeslice (
  id serial NOT NULL,
  feature_uuid text REFERENCES aixm_feature ON DELETE CASCADE,
  binary_object bytea,
  attr_gml_id text,
  gml_validtime_gml_timeinstant timestamp,
  gml_validtime_gml_timeperiod_begin timestamp,
  gml_validtime_gml_timeperiod_end timestamp,
  aixm_interpretation text,
  aixm_sequence_number integer,
  aixm_correction_number integer,
  aixm_featurelifetime_gml_timeperiod_begin timestamp,
  aixm_featurelifetime_gml_timeperiod_end timestamp,
  CONSTRAINT aixm_timeslice_pkey PRIMARY KEY (id)
);