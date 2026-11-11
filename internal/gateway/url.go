package gateway

import (
	"fmt"
	"net/url"
	"strconv"
)

func withGatewayParams(raw string) (string, error) {
	u, err := url.Parse(raw)

	if err != nil {
		return "", fmt.Errorf("failed to parse gateway url: %w", err)
	}

	q := u.Query()

	if !q.Has("v") {
		q.Set("v", strconv.Itoa(Version))
	}

	if !q.Has("encoding") {
		q.Set("encoding", DefaultEncoding)
	}

	u.RawQuery = q.Encode()

	return u.String(), nil
}
