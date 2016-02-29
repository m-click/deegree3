CREATE TABLE airport_heliport (
  attr_gml_id text,
  gml_identifier text,
  CONSTRAINT airport_heliport_pkey PRIMARY KEY (attr_gml_id)
);
CREATE TABLE airspace (
  attr_gml_id text,
  gml_identifier text,
  CONSTRAINT airspace_pkey PRIMARY KEY (attr_gml_id)
);
CREATE TABLE vertical_structure (
  attr_gml_id text,
  gml_identifier text,
  CONSTRAINT vertical_structure_pkey PRIMARY KEY (attr_gml_id)
);