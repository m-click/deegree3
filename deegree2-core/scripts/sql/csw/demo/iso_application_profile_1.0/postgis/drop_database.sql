SELECT DropGeometryColumn('', 'ex_boundingpolygon','geom');
SELECT DropGeometryColumn('', 'ex_geogrbbox','geom');

DROP SEQUENCE AlternateTitle_ID_seq ;
DROP SEQUENCE CI_Address_ID_seq ;
DROP SEQUENCE CI_Citation_ID_seq ;
DROP SEQUENCE CI_Contact_ID_seq ;
DROP SEQUENCE CI_OnLineFunctionCode_ID_seq ;
DROP SEQUENCE CI_OnlineResource_ID_seq ;
DROP SEQUENCE CI_PresentationFormCode_ID_seq ;
DROP SEQUENCE CI_RespParty_ID_seq ;
DROP SEQUENCE CI_RoleCode_ID_seq ;
DROP SEQUENCE CI_Series_ID_seq ;
DROP SEQUENCE CSW_CouplingType_ID_seq ;
DROP SEQUENCE CSW_ServiceIdentification_ID_seq ;
DROP SEQUENCE DeliveryPoint_ID_seq ;
DROP SEQUENCE DQ_ConformanceResult_ID_seq ;
DROP SEQUENCE DQ_DataQuality_ID_seq ;
DROP SEQUENCE DQ_Element_ID_seq ;
DROP SEQUENCE DQ_QuantitativeResult_ID_seq ;
DROP SEQUENCE DS_AssociationTypeCode_ID_seq;
DROP SEQUENCE ElectronicMailAddress_ID_seq ;
DROP SEQUENCE EX_BoundingPolygon_ID_seq ;
DROP SEQUENCE EX_GeographicDescription_ID_seq ;
DROP SEQUENCE EX_GeogrBBox_ID_seq ;
DROP SEQUENCE EX_TemporalExtent_ID_seq ;
DROP SEQUENCE EX_VerticalExtent_ID_seq ;
DROP SEQUENCE Facsimile_ID_seq ;
DROP SEQUENCE FAILEDREQUESTS_ID_seq ;
DROP SEQUENCE FeatureTypes_ID_seq ;
DROP SEQUENCE FileIdentifier_ID_seq ;
DROP SEQUENCE HierarchylevelCode_ID_seq ;
DROP SEQUENCE HierarchylevelName_ID_seq ;
DROP SEQUENCE Keyword_ID_seq ;
DROP SEQUENCE Language_ID_seq ;
DROP SEQUENCE LI_ProcessStep_ID_seq ;
DROP SEQUENCE LI_Source_ID_seq ;
DROP SEQUENCE MD_AggregateInfo_ID_seq;
DROP SEQUENCE MD_ApplicationSchemaInformation_ID_seq ;
DROP SEQUENCE MD_BrowseGraphic_ID_seq ;
DROP SEQUENCE MD_CharacterSetCode_ID_seq ;
DROP SEQUENCE MD_ClassificationCode_ID_seq ;
DROP SEQUENCE MD_Constraints_ID_seq ;
DROP SEQUENCE MD_DataIdentification_ID_seq ;
DROP SEQUENCE MD_DigTransferOpt_ID_seq ;
DROP SEQUENCE MD_Distribution_ID_seq ;
DROP SEQUENCE MD_Distributor_ID_seq ;
DROP SEQUENCE MD_FeatCatDesc_ID_seq ;
DROP SEQUENCE MD_Format_ID_seq ;
DROP SEQUENCE MD_GeoObjTypeCode_ID_seq ;
DROP SEQUENCE MD_Identification_ID_seq ;
DROP SEQUENCE MD_Keywords_ID_seq ;
DROP SEQUENCE MD_KeywordTypeCode_ID_seq ;
DROP SEQUENCE MD_LegalConstraints_ID_seq ;
DROP SEQUENCE MD_MainFreqCode_ID_seq ;
DROP SEQUENCE MD_MaintenanceInformation_ID_seq ;
DROP SEQUENCE MD_MediumFormatCode_ID_seq ;
DROP SEQUENCE MD_MediumNameCode_ID_seq ;
DROP SEQUENCE MD_Metadata_ID_seq ;
DROP SEQUENCE MD_PortrayalCatRef_ID_seq ;
DROP SEQUENCE MD_ProgressCode_ID_seq ;
DROP SEQUENCE MD_Resolution_ID_seq ;
DROP SEQUENCE MD_RestrictionCode_ID_seq ;
DROP SEQUENCE MD_ScopeCode_ID_seq ;
DROP SEQUENCE MD_SecurityConstraints_ID_seq;
DROP SEQUENCE MD_SpatialRepTypeCode_ID_seq ;
DROP SEQUENCE MD_StandOrderProc_ID_seq ;
DROP SEQUENCE MD_TopicCategoryCode_ID_seq ;
DROP SEQUENCE MD_TopoLevelCode_ID_seq ;
DROP SEQUENCE MD_Usage_ID_seq ;
DROP SEQUENCE MD_VectorSpatialReprenstation_ID_seq ;
DROP SEQUENCE OperatesOn_ID_seq ;
DROP SEQUENCE OperationNames_ID_seq ;
DROP SEQUENCE OtherConstraints_ID_seq ;
DROP SEQUENCE PT_Locale_ID_seq;
DROP SEQUENCE PublicationDate_ID_seq ;
DROP SEQUENCE QuantitativeRes_Value_ID_seq ;
DROP SEQUENCE RS_Identifier_ID_seq ;
DROP SEQUENCE ServiceVersion_ID_seq ;
DROP SEQUENCE SV_DCPList_ID_seq ;
DROP SEQUENCE SV_OperationMetadata_ID_seq ;
DROP SEQUENCE SV_Parameter_ID_seq ;
DROP SEQUENCE Voice_ID_seq ;


DROP TABLE AlternateTitle ;
DROP TABLE CI_Address ;
DROP TABLE CI_Citation ;
DROP TABLE CI_Contact ;
DROP TABLE CI_OnLineFunctionCode ;
DROP TABLE CI_OnlineResource ;
DROP TABLE CI_PresentationFormCode ;
DROP TABLE CI_RespParty ;
DROP TABLE CI_RoleCode ;
DROP TABLE CI_Series ;
DROP TABLE CSW_CouplingType ;
DROP TABLE CSW_ServiceIdentification ;
DROP TABLE DeliveryPoint ;
DROP TABLE DQ_ConformanceResult ;
DROP TABLE DQ_DataQuality ;
DROP TABLE DQ_Element ;
DROP TABLE DQ_QuantitativeResult ;
DROP TABLE DS_AssociationTypeCode;
DROP TABLE ElectronicMailAddress ;
DROP TABLE EX_BoundingPolygon ;
DROP TABLE EX_GeographicDescription ;
DROP TABLE EX_GeogrBBox ;
DROP TABLE EX_TemporalExtent ;
DROP TABLE EX_VerticalExtent ;
DROP TABLE Facsimile ;
DROP TABLE FAILEDREQUESTS ;
DROP TABLE FeatureTypes ;
DROP TABLE FileIdentifier ;
DROP TABLE HierarchylevelCode ;
DROP TABLE HierarchylevelName ;
drop table jt_address_delivpoint;
drop table jt_address_email;
drop table jt_citation_presform;
DROP TABLE JT_Citation_RespParty ;
DROP TABLE JT_Citation_Ident;
DROP TABLE JT_DataIdent_CharSet ;
DROP TABLE JT_DataIdent_SpatialRepType ;
DROP TABLE JT_DataIdent_TopicCat ;
DROP TABLE JT_DigTransOpt_MediumFormat ;
DROP TABLE JT_DigTransOpt_MediumName ;
DROP TABLE JT_DigTransOpt_OnlineRes ;
DROP TABLE JT_Dist_DistFormat ;
DROP TABLE JT_Dist_Distributor ;
DROP TABLE JT_Ident_Const ;
DROP TABLE JT_Ident_Keywords ;
DROP TABLE JT_Ident_LegalConst ;
DROP TABLE JT_Ident_Mainten ;
DROP TABLE JT_Ident_Progress ;
DROP TABLE JT_Ident_RespParty ;
DROP TABLE JT_Ident_SecConst ;
DROP TABLE JT_Ident_Usage ;
DROP TABLE JT_Keywords_Keyword ;
DROP TABLE JT_LegalConst_accessConst ;
DROP TABLE JT_LegalConst_useConst ;
DROP TABLE JT_Procstep_RespParty;
DROP TABLE JT_LI_SRC_LI_PROCSTEP ;
DROP TABLE JT_Metadata_AppSchemaInf ;
DROP TABLE JT_Metadata_Const ;
DROP TABLE JT_Metadata_FeatCatDesc ;
DROP TABLE JT_Metadata_LegalConst ;
drop table jt_metadata_locale;
DROP TABLE JT_Metadata_PortCatRef ;
DROP TABLE JT_Metadata_RefSys ;
DROP TABLE JT_Metadata_RespParty ;
DROP TABLE JT_Metadata_SecConst ;
DROP TABLE JT_Operation_DCP ;
DROP TABLE JT_Operation_Name ;
DROP TABLE JT_Operation_OperatesOn ;
DROP TABLE JT_Operation_Parameter ;
DROP TABLE JT_OpMeta_OnlineRes ;
DROP TABLE JT_Quality_Procstep ;
DROP TABLE JT_SecConst_ClassificationCode ;
DROP TABLE JT_Usage_RespParty ;
DROP TABLE Keyword ;
DROP TABLE Language ;
DROP TABLE LI_ProcessStep ;
DROP TABLE LI_Source ;
DROP TABLE MD_AggregateInfo ;
DROP TABLE MD_ApplicationSchemaInformation ;
DROP TABLE MD_BrowseGraphic ;
DROP TABLE MD_CharacterSetCode ;
DROP TABLE MD_ClassificationCode ;
DROP TABLE MD_Constraints ;
DROP TABLE MD_DataIdentification ;
DROP TABLE MD_DigTransferOpt ;
DROP TABLE MD_Distribution ;
DROP TABLE MD_Distributor ;
DROP TABLE MD_FeatCatDesc ;
DROP TABLE MD_Format ;
DROP TABLE MD_GeoObjTypeCode ;
DROP TABLE MD_Identification ;
DROP TABLE MD_Keywords ;
DROP TABLE MD_KeywordTypeCode ;
DROP TABLE MD_LegalConstraints ;
DROP TABLE MD_MainFreqCode ;
DROP TABLE MD_MaintenanceInformation ;
DROP TABLE MD_MediumFormatCode ;
DROP TABLE MD_MediumNameCode ;
DROP TABLE MD_Metadata ;
DROP TABLE MD_PortrayalCatRef ;
DROP TABLE MD_ProgressCode ;
DROP TABLE MD_Resolution ;
DROP TABLE MD_RestrictionCode ;
DROP TABLE MD_ScopeCode ;
DROP TABLE MD_SecurityConstraints ;
DROP TABLE MD_SpatialRepTypeCode ;
DROP TABLE MD_StandOrderProc ;
DROP TABLE MD_TopicCategoryCode ;
DROP TABLE MD_TopoLevelCode ;
DROP TABLE MD_Usage ;
DROP TABLE MD_VectorSpatialReprenstation ;
DROP TABLE OperatesOn ;
DROP TABLE OperationNames ;
DROP TABLE OtherConstraints ;
DROP TABLE PT_Locale ;
DROP TABLE PublicationDate ;
DROP TABLE QuantitativeRes_Value ;
DROP TABLE RS_Identifier ;
DROP TABLE ServiceVersion ;
DROP TABLE SV_DCPList ;
DROP TABLE SV_OperationMetadata ;
DROP TABLE SV_Parameter ;
DROP TABLE Voice ;
