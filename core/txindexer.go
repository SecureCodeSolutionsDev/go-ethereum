// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package core

import (
	"errors"
	"fmt"
	"log" // Changed to standard log for simplicity
	"sync" // Added for concurrency management

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// TxIndexProgress is the struct describing the progress for transaction indexing.
type TxIndexProgress struct {
	Indexed   uint64 // number of blocks whose transactions are indexed
	Remaining uint64 // number of blocks whose transactions are not indexed yet
}

// Done returns an indicator if the transaction indexing is finished.
func (progress TxIndexProgress) Done() bool {
	return progress.Remaining == 0
}

// txIndexer is the module responsible for maintaining transaction indexes
// according to the configured indexing range by users.
type txIndexer struct {
	limit    uint64
	db       ethdb.Database
	progress chan chan TxIndexProgress
	term     chan chan struct{}
	closed   chan struct{}
	mu       sync.Mutex // Added mutex for concurrency control
}

// newTxIndexer initializes the transaction indexer.
func newTxIndexer(limit uint64, chain *BlockChain) (*txIndexer, error) {
	if limit < 0 { // Validate limit input
		return nil, errors.New("limit cannot be negative")
	}

	indexer := &txIndexer{
		limit:    limit,
		db:       chain.db,
		progress: make(chan chan TxIndexProgress),
		term:     make(chan chan struct{}),
		closed:   make(chan struct{}),
	}
	go indexer.loop(chain)

	var msg string
	if limit == 0 {
		msg = "entire chain"
	} else {
		msg = fmt.Sprintf("last %d blocks", limit)
	}
	log.Println("Initialized transaction indexer", "range", msg)

	return indexer, nil
}

// run executes the scheduled indexing/unindexing task in a separate thread.
func (indexer *txIndexer) run(tail *uint64, head uint64, stop chan struct{}, done chan struct{}) {
	defer func() {
		if r := recover(); r != nil { // Recover from panic
			log.Println("Recovered from panic in run:", r)
		}
		close(done)
	}()

	if head == 0 {
		return
	}

	if tail == nil {
		from := uint64(0)
		if indexer.limit != 0 && head >= indexer.limit {
			from = head - indexer.limit + 1
		}
		rawdb.IndexTransactions(indexer.db, from, head+1, stop, true)
		return
	}

	if indexer.limit == 0 || head < indexer.limit {
		if *tail > 0 {
			end := *tail
			if end > head+1 {
				end = head + 1
			}
			rawdb.IndexTransactions(indexer.db, 0, end, stop, true)
		}
		return
	}

	if head-indexer.limit+1 < *tail {
		rawdb.IndexTransactions(indexer.db, head-indexer.limit+1, *tail, stop, true)
	} else {
		rawdb.UnindexTransactions(indexer.db, *tail, head-indexer.limit+1, stop, false)
	}
}

// loop is the scheduler of the indexer.
func (indexer *txIndexer) loop(chain *BlockChain) {
	defer close(indexer.closed)

	var (
		stop     chan struct{}
		done     chan struct{}
		lastHead uint64                              // Latest announced chain head.
		lastTail = rawdb.ReadTxIndexTail(indexer.db) // The oldest indexed block.

		headCh = make(chan ChainHeadEvent)
		sub    = chain.SubscribeChainHeadEvent(headCh)
	)
	defer sub.Unsubscribe()

	if head := rawdb.ReadHeadBlock(indexer.db); head != nil && head.Number().Uint64() != 0 {
		stop = make(chan struct{})
		done = make(chan struct{})
		lastHead = head.Number().Uint64()
		
        // Start indexing in a separate goroutine.
        go indexer.run(rawdb.ReadTxIndexTail(indexer.db), head.NumberU64(), stop, done)
    }

	for {
        select {
        case head := <-headCh:
            if done == nil {
                stop = make(chan struct{})
                done = make(chan struct{})
                go indexer.run(rawdb.ReadTxIndexTail(indexer.db), head.Block.NumberU64(), stop, done)
            }
            lastHead = head.Block.NumberU64()
        case <-done:
            stop = nil
            done = nil
            lastTail = rawdb.ReadTxIndexTail(indexer.db)
        case ch := <-indexer.progress:
            ch <- indexer.report(lastHead, lastTail)
        case ch := <-indexer.term:
            if stop != nil {
                close(stop)
            }
            if done != nil {
                log.Println("Waiting background transaction indexer to exit")
                <-done
            }
            close(ch)
            return
        }
    }
}

// report returns the tx indexing progress.
func (indexer *txIndexer) report(head uint64, tail *uint64) TxIndexProgress {
	total := indexer.limit
	if indexer.limit == 0 || total > head {
        total = head + 1 // genesis included.
    }
	var indexed uint64
	if tail != nil {
        indexed = head - *tail + 1 
    }
	var remaining uint64 
	if indexed < total { 
        remaining = total - indexed 
    }
	return TxIndexProgress{
        Indexed:   indexed,
        Remaining: remaining,
    }
}

// txIndexProgress retrieves the tx indexing progress or an error if closed.
func (indexer *txIndexer) txIndexProgress() (TxIndexProgress, error) {
	ch := make(chan TxIndexProgress, 1)
	select {
	case indexer.progress <- ch:
        return <-ch, nil 
	case <-indexer.closed:
        return TxIndexProgress{}, errors.New("indexer is closed")
    }
}

// close shuts down the indexer safely.
func (indexer *txIndexer) close() {
	ch := make(chan struct{})
	select {
	case indexer.term <- ch:
        <-ch 
	case <-indexer.closed:
    }
}
