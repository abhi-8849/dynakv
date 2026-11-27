package main

import (
	"fmt"
	"strconv"
	"strings"
)

// byteSize is a flag value that accepts plain bytes or a number with a KB, MB or GB suffix.
type byteSize int64

func (b *byteSize) String() string { return strconv.FormatInt(int64(*b), 10) }

func (b *byteSize) Set(s string) error {
	s = strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	for suffix, m := range map[string]int64{"KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30} {
		if strings.HasSuffix(s, suffix) {
			s, mult = strings.TrimSuffix(s, suffix), m
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return fmt.Errorf("invalid size %q", s)
	}
	*b = byteSize(n * mult)
	return nil
}
