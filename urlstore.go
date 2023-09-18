package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

const saveQueueLength = 1000

// URLStore is a map that stores URLs and their shortened forms.
type URLStore struct {
	urls  map[string]string
	mu    sync.RWMutex
	saver chan record
}

// URLStore records
type record struct {
	Key, URL string
}

// NewURLStore creates a new URLStore.
func NewURLStore(filename string) *URLStore {
	store := &URLStore{
		urls:  make(map[string]string),
		saver: make(chan record, saveQueueLength),
	}

	// load the file
	if err := store.load(filename); err != nil {
		log.Println("Error loading data store:", err)
	}

	go store.saveLoop(filename)
	return store
}

// Get returns the URL to which the given short URL maps.
func (s *URLStore) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.urls[key]
}

// Set saves the given URL to the store and returns the corresponding short URL.
func (s *URLStore) Set(key, url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if the URL is already in the store.
	if _, present := s.urls[key]; present {
		return false
	}
	s.urls[key] = url
	return true
}

func (s *URLStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.urls)
}

func (s *URLStore) Put(url string) (string, error) {
	key := genKey(url)
	if ok := s.Set(key, url); !ok {
		return "", fmt.Errorf("error setting key in URLStore")
	}

	s.saver <- record{key, url}

	return key, nil
}

// Saves records to file
func (s *URLStore) saveLoop(filename string) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		log.Fatal("Error opening URLStore: ", err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)

	for {
		record := <-s.saver

		if err := encoder.Encode(record); err != nil {
			log.Println("Error saving to URLStore: ", err)
		}
	}
}

func (s *URLStore) load(filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal("Error opening URLStore: ", err)
	}
	defer f.Close()

	if _, err = f.Seek(0, 0); err != nil {
		return err
	}
	d := json.NewDecoder(f)
	//var err error
	for err == nil {
		var r record

		if err = d.Decode(&r); err == nil {
			s.Set(r.Key, r.URL)
		}
	}
	if err == io.EOF {
		return nil
	}

	return err
}

func genKey(url string) string {
	// takes hash of the key
	return fmt.Sprintf("%x", sha256.Sum256([]byte(url)))
}
