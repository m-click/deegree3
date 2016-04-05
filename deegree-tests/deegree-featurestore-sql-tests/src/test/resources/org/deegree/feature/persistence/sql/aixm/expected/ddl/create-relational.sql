CREATE TABLE airport_heliport (
  attr_gml_id text,
  gml_identifier text,
  aixm_feature_metadata bytea,
  CONSTRAINT airport_heliport_pkey PRIMARY KEY (attr_gml_id)
);
CREATE TABLE airport_heliport_timeslice (
  id serial NOT NULL,
  fk_airport text REFERENCES airport_heliport ON DELETE CASCADE,
  aixm_timeslice bytea,
  CONSTRAINT airport_heliport_timeslice_pkey PRIMARY KEY (id)
);
CREATE TABLE airspace (
  attr_gml_id text,
  gml_identifier text,
  aixm_feature_metadata bytea,
  CONSTRAINT airspace_pkey PRIMARY KEY (attr_gml_id)
);
CREATE TABLE airspace_timeslice (
  id serial NOT NULL,
  fk_airspace text REFERENCES airspace ON DELETE CASCADE,
  aixm_timeslice bytea,
  CONSTRAINT airspace_timeslice_pkey PRIMARY KEY (id)
);
CREATE TABLE vertical_structure (
  attr_gml_id text,
  gml_identifier text,
  aixm_feature_metadata bytea,
  CONSTRAINT vertical_structure_pkey PRIMARY KEY (attr_gml_id)
);
CREATE TABLE vertical_structure_timeslice (
  id serial NOT NULL,
  fk_vertical_structure text REFERENCES vertical_structure ON DELETE CASCADE,
  aixm_timeslice bytea,
  attr_gml_id text,
  gml_validtime_gml_timeinstant timestamp,
  gml_validtime_gml_timeperiod_begin timestamp,
  gml_validtime_gml_timeperiod_end timestamp,
  aixm_interpretation text,
  aixm_sequence_number integer,
  aixm_correction_number integer,
  aixm_featurelifetime_gml_timeperiod_begin timestamp,
  aixm_featurelifetime_gml_timeperiod_end timestamp,
  name_text text,
  type_text text,
  lighted_text text,
  group_text text,
  CONSTRAINT vertical_structure_timeslice_pkey PRIMARY KEY (id)
);
CREATE TABLE vertical_structure_timeslice_part (
  id serial,
  fk_timeslice integer REFERENCES vertical_structure_timeslice ON DELETE CASCADE,
  pos integer NOT NULL,
  attr_gml_id text,
  vertical_extent numeric,
  vertical_extent_uom text,
  type_text text,
  mobile_text text,
  horizontalprojection_location_elevation text,
  horizontalprojection_location_elevation_uom text,
  CONSTRAINT vertical_structure_timeslice_part_pkey PRIMARY KEY (id)
);
SELECT ADDGEOMETRYCOLUMN('', 'vertical_structure_timeslice_part', 'horizontalprojection_location_point', 0, 'GEOMETRY', 2);