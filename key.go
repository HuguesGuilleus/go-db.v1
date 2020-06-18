// Copyright (c) 2020, HuguesGuilleus. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"strconv"
)

// An number type key.
type Key uint32

/* STRING */

// Get the key string in decimal format.
func (k Key) String() string {
	return strconv.FormatUint(uint64(k), 10)
}

// Convert a string who represent a decimal number to a key.
// If error the key value if zero.
func KeyFromString(s string) Key {
	u, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return Key(0)
	}
	return Key(u)
}

/* BYTES */

// Get the key used in the DB in bytes format.
// It begin by "@k" to make a difference between number key and
// string key. Beyond the number in little-endian format.
func (k Key) bytes() []byte {
	return []byte{'@', 'k',
		byte(k),
		byte(k >> 8),
		byte(k >> 16),
		byte(k >> 24),
	}
}

// Convert a key from the BD to a Key
func keyBytes(bytes []byte) Key {
	if len(bytes) != 6 || (bytes[0] != '@') || (bytes[1] != 'k') {
		return Key(0)
	}
	return Key(bytes[2]) +
		Key(bytes[3])<<8 +
		Key(bytes[4])<<16 +
		Key(bytes[5])<<24
}
