package internal

import "testing"

func TestParseURL(t *testing.T) {
	tests := []struct {
		URL         string
		Expected    string
		ExpectError bool
	}{
		{"tcp://host", "tcp://host:1883", false},
		{"tcp://host:999", "tcp://host:999", false},
		{"ssl://host", "ssl://host:8883", false},
		{"ssl://host:999", "ssl://host:999", false},
		{"http://host", "", true},
	}
	for _, test := range tests {
		url, err := parseURL(test.URL)
		if err != nil {
			if !test.ExpectError {
				t.Errorf("did not expect error for %s, got %s", test.URL, err)
			}
		} else {
			if test.ExpectError && err == nil {
				t.Errorf("expected error for %s", test.URL)
			}
			if url.String() != test.Expected {
				t.Errorf("expected %s, got %s", test.Expected, url)
			}
		}
	}
}
