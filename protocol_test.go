package bmemcached

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	key := "abc"
	conn, err := New("localhost:11211")
	if err != nil {
		t.Fatal(err)
	}
	size, err := conn.Set(key, "def", 1000)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, size, 3)
	conn.Delete(key)
}

func TestGet(t *testing.T) {
	key := "abc"
	conn, err := New("localhost:11211")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Set(key, "def", 1000)
	if err != nil {
		t.Fatal(err)
	}
	value, err := conn.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, value, "def")
	conn.Delete(key)
}

func TestDelete(t *testing.T) {
	key := "abc"
	conn, err := New("localhost:11211")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Set(key, "def", 1000)
	if err != nil {
		t.Fatal(err)
	}
	value, err := conn.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, value, "def")
	conn.Delete(key)
	value, err = conn.Get(key)
	assert.Equal(t, err.Error(), "Key not found")
}
