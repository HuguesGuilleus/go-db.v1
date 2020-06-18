// Copyright (c) 2020, HuguesGuilleus. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKeyToString(t *testing.T) {
	var k Key = 305419896
	assert.Equal(t, "305419896", k.String())
}

func TestKeyFromString(t *testing.T) {
	assert.Equal(t, KeyFromString("305419896"), Key(305419896))
	assert.Equal(t, KeyFromString("sdfb"), Key(0))
	assert.Equal(t, KeyFromString(""), Key(0))
}

func TestKeyToBytes(t *testing.T) {
	assert.Equal(t, []byte("@k\x78\x56\x34\x12"), Key(0x12345678).bytes())
}

func TestKeyFromBytes(t *testing.T) {
	assert.Equal(t, Key(0x12345678), keyBytes([]byte("@k\x78\x56\x34\x12")))
	assert.Equal(t, Key(0), keyBytes([]byte("@x\x78\x56\x34\x12")))
	assert.Equal(t, Key(0), keyBytes([]byte("xk\x78\x56\x34\x12")))
	assert.Equal(t, Key(0), keyBytes(nil))
}
