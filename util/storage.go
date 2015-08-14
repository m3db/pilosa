package util

import (
	"io"
	"os"
	"strings"

	"github.com/kr/s3/s3util"
)

func Open(s string) (io.ReadCloser, error) {
	if isURL(s) {
		return s3util.Open(s, nil)
	}
	return os.Open(s)
}

func Create(s string) (io.WriteCloser, error) {
	if isURL(s) {
		return s3util.Create(s, nil, nil)
	}
	return os.Create(s)
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}
