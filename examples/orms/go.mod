module github.com/code19m/taskmill/examples/orms

go 1.26.0

toolchain go1.27.4

require (
	entgo.io/ent v0.14.6
	github.com/code19m/taskmill v0.12.2
	github.com/jackc/pgx/v5 v5.13.0
	gorm.io/driver/postgres v1.6.3
	gorm.io/gorm v1.31.2
)

require (
	ariga.io/atlas v0.36.2-0.20250730182955-2c6300d0a3e1 // indirect
	github.com/agext/levenshtein v1.2.3 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/bmatcuk/doublestar v1.3.4 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/hcl/v2 v2.18.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	github.com/zclconf/go-cty v1.14.4 // indirect
	github.com/zclconf/go-cty-yaml v1.1.0 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Builds in this repository use the core next to the module, so that the
// examples are tested at the same commit.
replace github.com/code19m/taskmill => ../..
