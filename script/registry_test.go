package script

import (
	"strings"
	"testing"
)

func TestLoadEmbedded(t *testing.T) {
	reg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, name := range []string{"lock_release", "lock_renew"} {
		s, ok := reg.Get(name)
		if !ok {
			t.Fatalf("script %q not loaded", name)
		}
		if s.Version != 1 || len(s.SHA1) != 40 || !s.Idempotent {
			t.Fatalf("script %q metadata wrong: %+v", name, s)
		}
	}
}

func TestParseRejectsForbiddenCalls(t *testing.T) {
	bad := `-- @name evil
-- @version 1
-- @keys_desc KEYS[1]=k(router)
-- @idempotent true
return redis.call('SCAN', 0)
`
	_, err := parse("evil.lua", bad)
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("expected forbidden-call rejection, got %v", err)
	}
}

func TestParseRequiresHeaders(t *testing.T) {
	_, err := parse("noheader.lua", "return 1\n")
	if err == nil {
		t.Fatal("expected missing-header rejection")
	}
}

func TestParseRequiresRouterKey(t *testing.T) {
	src := `-- @name norouter
-- @version 1
-- @keys_desc no keys declared
-- @idempotent true
return 1
`
	if _, err := parse("norouter.lua", src); err == nil {
		t.Fatal("expected KEYS[1] router-key rejection")
	}
}
