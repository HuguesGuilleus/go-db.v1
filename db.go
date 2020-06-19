// Copyright (c) 2020, HuguesGuilleus. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"bytes"
	"encoding/gob"
	"github.com/prologic/bitcask"
	"log"
	"reflect"
	"sort"
)

// DB is a data base
type DB struct {
	intern   *bitcask.Bitcask
	maxIndex Key // The max id
	name     string
}

// Open a new DabatBase
func New(name string, opt ...bitcask.Option) (db *DB) {
	intern, err := bitcask.Open(name, opt...)
	if err != nil {
		log.Println("[DB ERROR]", err)
	}

	db = &DB{
		intern: intern,
		name:   name,
	}

	db.intern.Fold(func(key []byte) error {
		if k := keyBytes(key); k > db.maxIndex {
			db.maxIndex = k
		}
		return nil
	})

	return
}

// Print the error if the error is not nil and return true.
// If no error return false.
func (db *DB) print(err error, key []byte) bool {
	if err != nil {
		if len(key) > 20 {
			key = key[:20]
		}
		log.Printf("[DB ERROR] %q <key:%x>: %v\n", db.name, key, err)
	}
	return false
}

/* BASIC MANIPULATION */

// Create a new number index in the DB.
func (db *DB) New() Key {
	db.maxIndex++
	return db.maxIndex - 1
}

// Return true if the key is unknown in the DB.
func (db *DB) Unknown(key Key) bool {
	return !db.intern.Has(key.bytes())
}
func (db *DB) UnknownS(key string) bool {
	return !db.intern.Has([]byte(key))
}

// Delete on element
func (db *DB) Delete(key Key) {
	k := key.bytes()
	db.print(db.intern.Delete(k), k)
}

// Delete on element
func (db *DB) DeleteS(key string) {
	k := []byte(key)
	db.print(db.intern.Delete(k), k)
}

// Remove all keys in the DB.
func (db *DB) DeleteAll() {
	db.maxIndex = 0
	db.print(db.intern.DeleteAll(), nil)
}

/* GET */

// Get the element with a number key and save it in v.
func (db *DB) Get(key Key, v interface{}) (noExist bool) {
	return db.parse(key.bytes(), v)
}

// Get the element with a string key and save it in v.
func (db *DB) GetS(key string, v interface{}) (noExist bool) {
	return db.parse([]byte(key), v)
}

// Get the value from the DB, decode and save it the v.
func (db *DB) parse(key []byte, v interface{}) (noExist bool) {
	// Set v to Zero
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))

	// get the value
	data, err := db.intern.Get(key)
	if err == bitcask.ErrKeyNotFound {
		return true
	} else if db.print(err, key) {
		return true
	}

	// Decode the value
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(v)
	if db.print(err, key) {
		db.intern.Delete(key)
		return true
	}

	return false
}

// Get the value in without decoding with a number key.
func (db *DB) GetRaw(key Key) []byte {
	k := key.bytes()
	data, err := db.intern.Get(k)
	if err != bitcask.ErrKeyNotFound {
		return nil
	}
	db.print(err, k)
	return data
}

// Get the value in without decoding with a string key.
func (db *DB) GetSRaw(key string) []byte {
	k := []byte(key)
	data, err := db.intern.Get(k)
	if err != bitcask.ErrKeyNotFound {
		return nil
	}
	db.print(err, k)
	return data
}

/* FOR */

// Make an iteration on all the element in the DB that begins with prefix.
//
// Page is the index of the page of length size. If zero, web take all keys.
//
// We can select some elements with them key, so we use filter.
// filter can be nil.
//
// It must be a function that take a string and a other type for the value,
// else it panic.
//	MyDB.For(func(k db.Key, v MyType){...})
//
// total is the number of elements in the DB with prefix and that pass filter.
func (db *DB) ForS(prefix string, page, size int, filter func(string) bool, it interface{}) (total int) {
	f := reflect.ValueOf(it)
	t := f.Type()
	if t.Kind() != reflect.Func ||
		t.NumIn() != 2 ||
		t.In(0) != reflect.TypeOf("") {
		log.Panic("DB.ForS() need a iteration function")
	}
	v := reflect.New(t.In(1)).Elem()

	if filter == nil {
		filter = func(string) bool { return true }
	}

	// TODO: optimize me
	all := make([]string, 0)
	db.intern.Scan([]byte(prefix), func(key []byte) error {
		k := string(key)
		if filter(k) {
			total++
			all = append(all, k)
		}
		return nil
	})

	// Get a page
	if size != 0 {
		sort.Strings(all)
		begin := page * size
		if begin < 0 || begin > len(all) {
			begin = 0
		}
		end := size + begin
		if end > len(all) || end < begin {
			end = len(all)
		}
		all = all[begin:end]
	}

	for _, key := range all {
		k := []byte(key)
		data, err := db.intern.Get(k)
		if db.print(err, k) {
			continue
		}

		v.Set(reflect.Zero(v.Type()))
		err = gob.NewDecoder(bytes.NewReader(data)).DecodeValue(v)
		if db.print(err, k) {
			continue
		}

		f.Call([]reflect.Value{reflect.ValueOf(string(key)), v})
	}

	return
}

/* SET */

// Save with a number key, the value v in the DB with serialization.
func (db *DB) Set(key Key, v interface{}) {
	db.set(key.bytes(), v)
}

// Save with a string key, the value v in the DB with serialization.
func (db *DB) SetS(key string, v interface{}) {
	db.set([]byte(key), v)
}

// Serialization the value v and save it in the DB.
func (db *DB) set(k []byte, v interface{}) {
	w := bytes.Buffer{}
	if db.print(gob.NewEncoder(&w).Encode(v), k) {
		return
	}
	db.print(db.intern.Put(k, w.Bytes()), k)
}

// Set with a number key, the value v without serialization.
func (db *DB) SetRaw(key Key, raw []byte) {
	k := key.bytes()
	db.print(db.intern.Put(k, raw), k)
}

// Set with a string key, the value v without serialization.
func (db *DB) SetSRaw(key string, raw []byte) {
	k := []byte(key)
	db.print(db.intern.Put(k, raw), k)
}
