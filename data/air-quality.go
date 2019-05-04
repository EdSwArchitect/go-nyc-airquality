package airquality

// AirQuality data type
type AirQuality struct {
	IndicatorDataID  string `json:"indicator_data_id"`
	IndicatorID      string `json:"indicator_id"`
	Name             string `json:"name"`
	Measure          string `json:"measure"`
	GeoTypeName      string `json:"geo_type_name"`
	GeoEntityID      string `json:"geo_entity_id"`
	GeoEntityName    string `json:"geo_entity_name"`
	YearDescription  string `json:"year_description"`
	DataValueMessage string `json:"data_value_message"`
}

// SearchField base for enum
type SearchField int

// the enum declaration
const (
	Undef SearchField = iota
	Name              //SearchField = iota + 1
	Measure
	GeoTypeName
	GeoEntityID
	GeoEntityName
	YearDescription
	DataValueMessage
)

// String value for the field
func (field SearchField) String() string {
	fields := [...]string{
		"Name",
		"Measure",
		"GeoTypeName",
		"GeoEntityID",
		"GeoEntityName",
		"YearDescription",
		"DataValueMessage"}

	if field < Name || field > DataValueMessage {
		return "Undef"
	}

	return fields[field]
}

// GetNewField will return the enumerated type for the string
func (field SearchField) GetNewField(name string) SearchField {
	data := map[string]SearchField{
		"Undef":            Undef,
		"Name":             Name,
		"Measure":          Measure,
		"GeoTypeName":      GeoTypeName,
		"GeoEntityID":      GeoEntityID,
		"GeoEntityName":    GeoEntityName,
		"YearDescription":  YearDescription,
		"DataValueMessage": DataValueMessage,
	}

	var v SearchField

	v = data[name]

	return v

}

// GetField will return the enumerated type for the string
func GetField(name string) SearchField {
	data := map[string]SearchField{
		"Undef":            Undef,
		"Name":             Name,
		"Measure":          Measure,
		"GeoTypeName":      GeoTypeName,
		"GeoEntityID":      GeoEntityID,
		"GeoEntityName":    GeoEntityName,
		"YearDescription":  YearDescription,
		"DataValueMessage": DataValueMessage,
	}

	var v SearchField

	v = data[name]

	return v
}

// GetField will return the enumerated type for the string
func (field SearchField) GetField(name string) SearchField {
	data := map[string]SearchField{
		"Undef":            Undef,
		"Name":             Name,
		"Measure":          Measure,
		"GeoTypeName":      GeoTypeName,
		"GeoEntityID":      GeoEntityID,
		"GeoEntityName":    GeoEntityName,
		"YearDescription":  YearDescription,
		"DataValueMessage": DataValueMessage,
	}

	var v SearchField

	v = data[name]

	return v

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
