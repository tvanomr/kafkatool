package main

import (
	"fmt"
	"strconv"
	"strings"
)

func parseBinarySize(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) == 0 {
		return 0, fmt.Errorf("number expected")
	}
	last := trimmed[len(trimmed)-1]
	factor := int64(1)
	switch last {
	case 'K':
		factor = 1024
	case 'M':
		factor = 1024 * 1024
	case 'G':
		factor = 1024 * 1024 * 1024
	case 'T':
		factor = 1024 * 1024 * 1024 * 1024
	default:
		return strconv.ParseInt(trimmed, 10, 64)
	}
	intValue, err := strconv.ParseInt(trimmed[:len(trimmed)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	return intValue * factor, err
}
