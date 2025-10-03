package storage

import "fmt"

// method signatures
type Storage interface {
	PutDocument(documentPath string) (int64, error)
	GetDocument(documentNumber int64) (string, error)
	UpdateIndex(documentNumber int64, wordFrequencies map[string]int64) error
	LookupIndex(term string) ([]DocFreqPair, error)
}

// struct for storing document numbers and their corresponding word frequencies
type DocFreqPair struct {
	DocumentNumber int64
	WordFrequency  int64
}

// struct for managing document indexing with inverted index data structures
type IndexStore struct {
	documentMap       map[string]int64
	termInvertedIndex map[string][]DocFreqPair
	documentNumCount  int64
}

// create a new IndexStore
func NewIndexStore() *IndexStore {
	return &IndexStore{
		documentMap:       make(map[string]int64),
		termInvertedIndex: make(map[string][]DocFreqPair),
		documentNumCount:  0,
	}
}

// assign document paths a unique number for DocumentMap data structure
func (s *IndexStore) PutDocument(documentPath string) (int64, error) {
	if documentPath == "" {
		return 0, fmt.Errorf("document path cannot be empty")
	}

	uniqueNumber := s.documentNumCount
	s.documentMap[documentPath] = uniqueNumber
	s.documentNumCount++

	return uniqueNumber, nil
}

// retreive the document path by giving the corresponding document number
func (s *IndexStore) GetDocument(documentNumber int64) (string, error) {
	for path, num := range s.documentMap {
		if num == documentNumber {
			return path, nil
		}
	}
	return "", fmt.Errorf("document with number %d not found", documentNumber)
}

// update the the index with additional word frequency pairs for the specified document number
func (s *IndexStore) UpdateIndex(documentNumber int64, wordFrequencies map[string]int64) error {
	if wordFrequencies == nil {
		return fmt.Errorf("word frequencies cannot be nil")
	}

	for term, freq := range wordFrequencies {
		if value, ok := s.termInvertedIndex[term]; ok {
			pair := DocFreqPair{DocumentNumber: documentNumber, WordFrequency: freq}
			s.termInvertedIndex[term] = append(value, pair)
		} else {
			pair := DocFreqPair{DocumentNumber: documentNumber, WordFrequency: freq}
			s.termInvertedIndex[term] = []DocFreqPair{pair}
		}
	}
	return nil
}

// retreive the DocFreqPairs for the specified term in the index
func (s *IndexStore) LookupIndex(term string) ([]DocFreqPair, error) {
	results := []DocFreqPair{}

	if term == "" {
		return nil, fmt.Errorf("term cannot be empty")
	}

	if value, ok := s.termInvertedIndex[term]; ok {
		results = value
	}

	return results, nil
}
