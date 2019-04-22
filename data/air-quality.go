package airquality

// AirQuality data type
type AirQuality struct {
	IndicatorDataID  int32   `json:"indicator_data_id"`
	IndicatorID      int32   `json:"indicator_id"`
	Name             string  `json:"name"`
	Measure          string  `json:"measure"`
	GeoTypeName      string  `json:"geo_type_name"`
	GeoEntityID      int32   `json:"geo_entity_id"`
	GeoEntityName    string  `json:"geo_entity_name"`
	YearDescription  string  `json:"year_description"`
	DataValueMessage float32 `json:"data_value_message"`
}

// indicator_data_id,  130728
// indicator_id, 646
// name, Air Toxics Concentrations- Average Benzene Concentrations
// Measure, Average Concentration
// geo_type_name, Borough
// geo_entity_id, 1
// geo_entity_name, Bronx
// year_description, 2005
// data_valuemessage 2.8
