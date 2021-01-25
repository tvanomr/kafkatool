package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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

func parseDuration(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(value)
	var result time.Duration
	var number int
	var readNumber = func() {
		number = 0
		for trimmed[0] >= '0' && trimmed[0] <= '9' {
			number *= 10
			number += int(trimmed[0] - '0')
			trimmed = trimmed[1:]
		}
	}
	readNumber()
	if trimmed[0] == 'w' {
		result += time.Duration(number*7*24) * time.Hour
		trimmed = trimmed[1:]
		if len(trimmed) == 0 {
			return result, nil
		}
		readNumber()
	}
	if trimmed[0] == 'd' {
		result += time.Duration(number*24) * time.Hour
		trimmed = trimmed[1:]
		if len(trimmed) == 0 {
			return result, nil
		}
		readNumber()
	}
	if trimmed[0] == 'h' {
		result += time.Duration(number) * time.Hour
		trimmed = trimmed[1:]
		if len(trimmed) == 0 {
			return result, nil
		}
		readNumber()
	}
	if trimmed[0] == 'm' {
		if len(trimmed) == 2 && trimmed[1] == 's' { //ms
			result += time.Duration(number) * time.Millisecond
			return result, nil
		}
		result += time.Duration(number) * time.Minute
		trimmed = trimmed[1:]
		if len(trimmed) == 0 {
			return result, nil
		}
		readNumber()
	}
	if trimmed[0] == 's' {
		result += time.Duration(number) * time.Second
		trimmed = trimmed[1:]
		if len(trimmed) == 0 {
			return result, nil
		}
		readNumber()
	}
	if len(trimmed) == 2 && trimmed[0] == 'm' && trimmed[1] == 's' {
		result += time.Duration(number) * time.Millisecond
		return result, nil
	}
	return time.Duration(0), fmt.Errorf("unexpected suffix, %s", trimmed)
}
