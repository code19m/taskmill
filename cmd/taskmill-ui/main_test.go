package main

import "testing"

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	env := func(vars map[string]string) func(string) string {
		return func(k string) string { return vars[k] }
	}

	if _, err := loadConfig(env(nil)); err == nil {
		t.Fatal("no database URL was accepted")
	}

	c, err := loadConfig(env(map[string]string{"TASKMILL_DATABASE_URL": "postgres://db"}))
	if err != nil || c.addr != "localhost:8080" || c.auth != nil || c.readOnly {
		t.Fatalf("defaults: got %+v, %v", c, err)
	}

	c, err = loadConfig(env(map[string]string{
		"TASKMILL_DATABASE_URL": "postgres://db", "TASKMILL_UI_ADDR": ":9000",
		"TASKMILL_UI_PASSWORD": "s3cret", "TASKMILL_UI_READ_ONLY": "true",
	}))
	if err != nil || c.addr != ":9000" || !c.readOnly || c.auth == nil || c.auth.Username != "admin" {
		t.Fatalf("got %+v, %v", c, err)
	}

	if _, err := loadConfig(env(map[string]string{"TASKMILL_DATABASE_URL": "postgres://db", "TASKMILL_UI_READ_ONLY": "maybe"})); err == nil {
		t.Fatal("an invalid read-only value was accepted")
	}
}
