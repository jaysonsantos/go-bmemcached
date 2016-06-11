package bmemcached

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	conn, err := New("localhost:11211")
	if err != nil {
		t.Error(err)
	}
	size, err := conn.Set("abc", "def", 1000)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, size, 3)
	conn.Delete("abc")
}

// func TestGet(t *testing.T) {
// 	conn, err := New("localhost:11211")
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	size, err := conn.Set("abc", "def", 1000)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	value, err := conn.Get("abc")

// }
