package dag

import (
	"sync"
	"sort"
)

// Set is a set data structure.
type Set struct {
	m    map[interface{}]interface{}
	once sync.Once
}

// Hashable is the interface used by set to get the hash code of a value.
// If this isn't given, then the value of the item being added to the set
// itself is used as the comparison value.
type Hashable interface {
	Hashcode() interface{}
}

// hashcode returns the hashcode used for set elements.
func hashcode(v interface{}) interface{} {
	if h, ok := v.(Hashable); ok {
		return h.Hashcode()
	}

	return v
}

// Add adds an item to the set
func (s *Set) Add(v interface{}) {
	s.once.Do(s.init)
	s.m[hashcode(v)] = v
}

// Delete removes an item from the set.
func (s *Set) Delete(v interface{}) {
	s.once.Do(s.init)
	delete(s.m, hashcode(v))
}

// Include returns true/false of whether a value is in the set.
func (s *Set) Include(v interface{}) bool {
	s.once.Do(s.init)
	_, ok := s.m[hashcode(v)]
	return ok
}

// Intersection computes the set intersection with other.
func (s *Set) Intersection(other *Set) *Set {
	result := new(Set)
	if s == nil {
		return result
	}
	if other != nil {
		for _, k := range s.getSortedKey() {
			if other.Include(s.m[k]) {
				result.Add(s.m[k])
			}
		}
	}

	return result
}

// Difference returns a set with the elements that s has but
// other doesn't.
func (s *Set) Difference(other *Set) *Set {
	result := new(Set)
	if s != nil {
		for _, k := range s.getSortedKey() {
			var ok bool
			if other != nil {
				_, ok = other.m[k]
			}
			if !ok {
				result.Add(s.m[k])
			}
		}
	}

	return result
}

// Filter returns a set that contains the elements from the receiver
// where the given callback returns true.
func (s *Set) Filter(cb func(interface{}) bool) *Set {
	result := new(Set)

	for _, k := range s.getSortedKey() {
		if cb(s.m[k]) {
			result.Add(s.m[k])
		}
	}

	return result
}

// Len is the number of items in the set.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}

	return len(s.m)
}

// List returns the list of set elements.
func (s *Set) List() []interface{} {
	if s == nil {
		return nil
	}

	r := make([]interface{}, 0, len(s.m))
	for _, k := range s.getSortedKey() {
		r = append(r, s.m[k])
	}

	return r
}

func (s *Set) init() {
	s.m = make(map[interface{}]interface{})
}

//Sort the keys to stabilize DAG execution
func (s *Set) getSortedKey() []interface{} {

	var ret []interface{}
	for k, _ := range s.m {
		ret = append(ret, k)
	}

	//only sort string temporarily
	sort.Slice(ret, func(i, j int) bool {
		a, okA := ret[i].(string)
		b, okB := ret[j].(string)

		if !okA || !okB {
			return i < j
		}

		return a < b
	})

	return ret
}
