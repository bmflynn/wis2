package internal

import (
	"regexp"
	"testing"
)

const (
	fixtureSha512 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	fixtureSha256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	fixtureMd5    = "ffffffffffffffffffffffffffffffff"
)

func TestMessage(t *testing.T) {
	t.Run("IsValid", func(t *testing.T) {
		tests := []struct {
			Name   string
			Msg    WISMessage
			ErrPat string
		}{
			{"sha256", WISMessage{BaseURL: "http://foo", RelPath: "/goo", Integrity: Integrity{"sha512", fixtureSha512}}, ""},
			{"sha512", WISMessage{BaseURL: "http://foo", RelPath: "/goo", Integrity: Integrity{"sha256", fixtureSha256}}, ""},
			{"md5", WISMessage{BaseURL: "http://foo", RelPath: "/goo", Integrity: Integrity{"md5", fixtureMd5}}, ""},
			{"works with retpath", WISMessage{BaseURL: "http://foo", RetPath: "/goo", Integrity: Integrity{"md5", fixtureMd5}}, ""},
			{"missing integrity is error", WISMessage{BaseURL: "http://foo", RelPath: "/goo"}, `unsupported integrity`},
			{"invalid integrity is error", WISMessage{BaseURL: "http://foo", RelPath: "/goo", Integrity: Integrity{"md5", "xx"}}, `invalid md5`},
		}

		for _, test := range tests {
			t.Run(test.Name, func(t *testing.T) {
				err := test.Msg.IsValid()
				if test.ErrPat == "" && err != nil {
					t.Errorf("did not expect error, got %s", err)
				}
				if test.ErrPat != "" && err != nil && !regexp.MustCompile(test.ErrPat).MatchString(err.Error()) {
					t.Errorf("expected error matching %s, got %s", test.ErrPat, err)
				}
				if test.ErrPat != "" && err == nil {
					t.Errorf("expected error, didn't get one")
				}
			})
		}
	})
}
