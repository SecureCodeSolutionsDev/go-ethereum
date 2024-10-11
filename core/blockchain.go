// Copyright 2014 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package core implements the Ethereum consensus protocol.
package core

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/internal/syncx"
	"github.com/ethereum/go-ethereum/internal/version"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

var (
	headBlockGauge          = metrics.NewRegisteredGauge("chain/head/block", nil)
	headHeaderGauge         = metrics.NewRegisteredGauge("chain/head/header", nil)
	headFastBlockGauge      = metrics.NewRegisteredGauge("chain/head/receipt", nil)
	headFinalizedBlockGauge = metrics.NewRegisteredGauge("chain/head/finalized", nil)
	headSafeBlockGauge      = metrics.NewRegisteredGauge("chain/head/safe", nil)

	chainInfoGauge = metrics.NewRegisteredGaugeInfo("chain/info", nil)

	accountReadTimer   = metrics.NewRegisteredResettingTimer("chain/account/reads", nil)
	accountHashTimer   = metrics.NewRegisteredResettingTimer("chain/account/hashes", nil)
	accountUpdateTimer = metrics.NewRegisteredResettingTimer("chain/account/updates", nil)
	accountCommitTimer = metrics.NewRegisteredResettingTimer("chain/account/commits", nil)

	storageReadTimer   = metrics.NewRegisteredResettingTimer("chain/storage/reads", nil)
	storageUpdateTimer = metrics.NewRegisteredResettingTimer("chain/storage/updates", nil)
	storageCommitTimer = metrics.NewRegisteredResettingTimer("chain/storage/commits", nil)

	accountReadSingleTimer = metrics.NewRegisteredResettingTimer("chain/account/single/reads", nil)
	storageReadSingleTimer = metrics.NewRegisteredResettingTimer("chain/storage/single/reads", nil)

	snapshotCommitTimer = metrics.NewRegisteredResettingTimer("chain/snapshot/commits", nil)
	triedbCommitTimer   = metrics.NewRegisteredResettingTimer("chain/triedb/commits", nil)

	blockInsertTimer          = metrics.NewRegisteredResettingTimer("chain/inserts", nil)
	blockValidationTimer      = metrics.NewRegisteredResettingTimer("chain/validation", nil)
	blockCrossValidationTimer = metrics.NewRegisteredResettingTimer("chain/crossvalidation", nil)
	blockExecutionTimer       = metrics.NewRegisteredResettingTimer("chain/execution", nil)
	blockWriteTimer           = metrics.NewRegisteredResettingTimer("chain/write", nil)

	blockReorgMeter     = metrics.NewRegisteredMeter("chain/reorg/executes", nil)
	blockReorgAddMeter  = metrics.NewRegisteredMeter("chain/reorg/add", nil)
	blockReorgDropMeter = metrics.NewRegisteredMeter("chain/reorg/drop", nil)

	blockPrefetchExecuteTimer   = metrics.NewRegisteredTimer("chain/prefetch/executes", nil)
	blockPrefetchInterruptMeter = metrics.NewRegisteredMeter("chain/prefetch/interrupts", nil)

	errInsertionInterrupted = errors.New("insertion is interrupted")
	errChainStopped         = errors.New("blockchain is stopped")
	errInvalidOldChain      = errors.New("invalid old chain")
	errInvalidNewChain      = errors.New("invalid new chain")
)

const (
	bodyCacheLimit     = 256
	blockCacheLimit    = 256
	receiptsCacheLimit = 32
	txLookupCacheLimit = 1024

	BlockChainVersion uint64 = 8 // Ensures that an incompatible database forces a resync from scratch.
)

// CacheConfig contains the configuration values for the trie database
// and state snapshot these are resident in a blockchain.
type CacheConfig struct {
	TrieCleanLimit      int           // Memory allowance (MB) to use for caching trie nodes in memory
	TrieCleanNoPrefetch bool          // Whether to disable heuristic state prefetching for followup blocks
	TrieDirtyLimit      int           // Memory limit (MB) at which to start flushing dirty trie nodes to disk
	TrieDirtyDisabled   bool          // Whether to disable trie write caching and GC altogether (archive node)
	TrieTimeLimit       time.Duration // Time limit after which to flush the current in-memory trie to disk
	SnapshotLimit       int           // Memory allowance (MB) to use for caching snapshot entries in memory
	Preimages           bool          // Whether to store preimage of trie key to the disk
	StateHistory        uint64        // Number of blocks from head whose state histories are reserved.
	StateScheme         string        // Scheme used to store ethereum states and merkle tree nodes on top

	SnapshotNoBuild bool // Whether the background generation is allowed
	SnapshotWait    bool // Wait for snapshot construction on startup. TODO(karalabe): This is a dirty hack for testing, nuke it
}

// triedbConfig derives the configures for trie database.
func (c *CacheConfig) triedbConfig(isVerkle bool) *triedb.Config {
	config := &triedb.Config{
		Preimages: c.Preimages,
		IsVerkle:  isVerkle,
	}
	if c.StateScheme == rawdb.HashScheme {
		config.HashDB = &hashdb.Config{
			CleanCacheSize: c.TrieCleanLimit * 1024 * 1024,
		}
	}
	if c.StateScheme == rawdb.PathScheme {
		config.PathDB = &pathdb.Config{
			StateHistory:   c.StateHistory,
			CleanCacheSize: c.TrieCleanLimit * 1024 * 1024,
			DirtyCacheSize: c.TrieDirtyLimit * 1024 * 1024,
		}
	}
	return config
}

// defaultCacheConfig are the default caching values if none are specified by the
// user (also used during testing).
var defaultCacheConfig = &CacheConfig{
	TrieCleanLimit: 256,
	TrieDirtyLimit: 256,
	TrieTimeLimit:  5 * time.Minute,
	SnapshotLimit:  256,
	SnapshotWait:   true,
	StateScheme:    rawdb.HashScheme,
}

// DefaultCacheConfigWithScheme returns a deep copied default cache config with
// a provided trie node scheme.
func DefaultCacheConfigWithScheme(scheme string) *CacheConfig {
	config := *defaultCacheConfig
	config.StateScheme = scheme
	return &config
}

// txLookup is wrapper over transaction lookup along with the corresponding
// transaction object.
type txLookup struct {
    lookup      *rawdb.LegacyTxLookupEntry
    transaction *types.Transaction
}

// BlockChain represents the canonical chain given a database with a genesis
// block. The Blockchain manages chain imports, reverts, chain reorganisations.
// Importing blocks in to the block chain happens according to the set of rules 
// defined by the two stage Validator. Processing of blocks is done using the 
// Processor which processes the included transaction. The validation of the state 
// is done in the second part of the Validator. Failing results in aborting of 
// the import.
// The BlockChain also helps in returning blocks from **any** chain included 
// in the database as well as blocks that represents the canonical chain. It's 
// important to note that GetBlock can return any block and does not need to be 
// included in the canonical one where as GetBlockByNumber always represents 
// the canonical chain.
type BlockChain struct {
    chainConfig *params.ChainConfig // Chain & network configuration
    cacheConfig *CacheConfig        // Cache configuration for pruning

    db            ethdb.Database                   // Low level persistent database to store final content in
    snaps         *snapshot.Tree                   // Snapshot tree for fast trie leaf access
    triegc        *prque.Prque[int64, common.Hash] // Priority queue mapping block numbers to tries to gc
    gcproc        time.Duration                    // Accumulates canonical block processing for trie dumping
    lastWrite     uint64                           // Last block when the state was flushed
    flushInterval atomic.Int64                     // Time interval (processing time) after which to flush a state
    triedb        *triedb.Database                 // The database handler for maintaining trie nodes.
    statedb       *state.CachingDB                 // State database to reuse between imports (contains state cache)
    txIndexer     *txIndexer                       // Transaction indexer, might be nil if not enabled

    hc            *HeaderChain
    rmLogsFeed    event.Feed
    chainFeed     event.Feed
    chainSideFeed event.Feed
    chainHeadFeed event.Feed
    logsFeed      event.Feed
    blockProcFeed event.Feed
    scope         event.SubscriptionScope
    genesisBlock  *types.Block

    // This mutex synchronizes chain write operations.
    // Readers don't need to take it, they can just read the database.
    chainmu *syncx.ClosableMutex

    currentBlock      atomic.Pointer[types.Header] // Current head of the chain
    currentSnapBlock  atomic.Pointer[types.Header] // Current head of snap-sync
    currentFinalBlock atomic.Pointer[types.Header] // Latest (consensus) finalized block
    currentSafeBlock  atomic.Pointer[types.Header] // Latest (consensus) safe block

    bodyCache     *lru.Cache[common.Hash, *types.Body]
    bodyRLPCache  *lru.Cache[common.Hash, rlp.RawValue]
    receiptsCache *lru.Cache[common.Hash, []*types.Receipt]
    blockCache    *lru.Cache[common.Hash, *types.Block]

    txLookupLock  sync.RWMutex
    txLookupCache *lru.Cache[common.Hash, txLookup]

    wg            sync.WaitGroup
    quit          chan struct{} // shutdown signal, closed in Stop.
    stopping      atomic.Bool   // false if chain is running, true when stopped
    procInterrupt atomic.Bool   // interrupt signaler for block processing

    engine     consensus.Engine
    validator  Validator // Block and state validator interface
    prefetcher Prefetcher 
    processor  Processor // Block transaction processor interface 
	vmConfig   vm.Config 
	logger     *tracing.Hooks 
}

// NewBlockChain returns a fully initialised block chain using information 
// available in the database. It initialises the default Ethereum Validator 
// and Processor.
func NewBlockChain(db ethdb.Database, cacheConfig *CacheConfig, genesis *Genesis, overrides *ChainOverrides, engine consensus.Engine, vmConfig vm.Config, txLookupLimit *uint64) (*BlockChain, error) {
	if cacheConfig == nil {
	    cacheConfig = defaultCacheConfig 
	    }
	
	triedb := triedb.NewDatabase(db, cacheConfig.triedbConfig(genesis != nil && genesis.IsVerkle()))

	    chainConfig, genesisHash, genesisErr := SetupGenesisBlockWithOverride(db, triedb, genesis, overrides) 
	
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
	    return nil, genesisErr 
	    }
	
	log.Info("") 
	log.Info(strings.Repeat("-", 153)) 
	
	for _, line := range strings.Split(chainConfig.Description(), "\n") { 
	    log.Info(line) 
	    } 
	
	log.Info(strings.Repeat("-", 153)) 
	log.Info("") 

	bc := &BlockChain{ 
	    chainConfig:   chainConfig,
	    cacheConfig:   cacheConfig,
	    db:            db,
	    triedb:        triedb,
	    triegc:        prque.New[int64, common.Hash](nil), 
	    quit:          make(chan struct{}),
	    chainmu:       syncx.NewClosableMutex(), 
	    bodyCache:     lru.NewCache[common.Hash, *types.Body](bodyCacheLimit), 
	    bodyRLPCache:  lru.NewCache[common.Hash, rlp.RawValue](bodyCacheLimit), 
	    receiptsCache: lru.NewCache[common.Hash, []*types.Receipt](receiptsCacheLimit), 
	    blockCache:    lru.NewCache[common.Hash, *types.Block](blockCacheLimit), 
	    txLookupCache: lru.NewCache[common.Hash, txLookup](txLookupCacheLimit), 
	    engine:        engine,
	    vmConfig:      vmConfig,
	    logger:        vmConfig.Tracer,
	    }
	
	var err error 
	
	bc.hc, err = NewHeaderChain(db, chainConfig, engine, bc.insertStopped) 
	
	if err != nil { return nil , err } 
	
	bc.flushInterval.Store(int64(cacheConfig.TrieTimeLimit)) 

	bc.statedb = state.NewDatabase(bc.triedb ,nil ) 

	bc.validator= NewBlockValidator(chainConfig ,bc ) 

	bc.prefetcher= newStatePrefetcher(chainConfig ,bc.hc ) 

	bc.processor= NewStateProcessor(chainConfig ,bc.hc ) 

	bc.genesisBlock= bc.GetBlockByNumber(0) 

	if bc.genesisBlock == nil { return nil , ErrNoGenesis } 

	bc.currentBlock.Store(nil ) 

	bc.currentSnapBlock.Store(nil ) 

	bc.currentFinalBlock.Store(nil ) 

	bc.currentSafeBlock.Store(nil ) 

	
.chainInfoGauge.Update(metrics.GaugeInfoValue {"chain_id": bc.chainConfig.ChainID.String()}) 


	if bc.empty() { rawdb.InitDatabaseFromFreezer(bc.db) } 

	if err := bc.loadLastState(); err != nil { return nil , err } 
	
	head := bc.CurrentBlock() 
	
	if !bc.HasState(head.Root) { if head.Number.Uint64() == 0 { log.Info("Genesis state is missing , wait state sync") } else { var diskRoot common.Hash
	
	if bc.cacheConfig.SnapshotLimit >0 { diskRoot= rawdb.ReadSnapshotRoot(bc.db) } if diskRoot != (common.Hash{}) { log.Warn( "Head state missing , repairing" ,"number" ,head.Number,"hash" ,head.Hash(),"snaproot" ,diskRoot )

				snapDisk ,err := bc.setHeadBeyondRoot(head.Number.Uint64(),0,diskRoot,true) 

				if err !=nil { return nil ,err } 

				if snapDisk !=0 { rawdb.WriteSnapshotRecoveryNumber(bc.db,snapDisk ) } } else { log.Warn( "Head state missing , repairing" ,"number" ,head.Number,"hash" ,head.Hash()) if _,err:=bc.setHeadBeyondRoot(head.Number.Uint64(),0 ,common.Hash{},true ); err !=nil { returnnil ,err } } } }

	
	if frozen ,err:=bc.db.Ancients(); err ==nil && frozen >0 { var ( needRewind bool low uint64 ) fullBlock := bc.CurrentBlock() if fullBlock !=nil && fullBlock.Hash() !=bc.genesisBlock.Hash() && fullBlock.Number.Uint64()<frozen -1 { needRewind=true low=fullBlock.Number.Uint64() }

				snapBlock:=bc.CurrentSnapBlock() if snapBlock !=nil && snapBlock.Number.Uint64()<frozen -1 { needRewind=true if snapBlock.Number.Uint64()<low || low==0 { low=snapBlock.Number.Uint64() } }

				if needRewind { log.Error( "Truncating ancient chain" ,"from" ,bc.CurrentHeader().Number.Uint64(),"to" ,low ) if err:=bc.SetHead(low); err!=nil{ returnnil ,err } } }

	
	bc.engine.VerifyHeader(bc ,bc.CurrentHeader()) 

	if bc.logger !=nil && bc.logger.OnBlockchainInit!=nil { bc.logger.OnBlockchainInit(chainConfig ) }

	if bc.logger !=nil && bc.logger.OnGenesisBlock!=nil { if block:=bc.CurrentBlock(); block.Number.Uint64()==0{ alloc ,err:=getGenesisState(bc.db ,block.Hash()) if err !=nil{ returnnil ,fmt.Errorf( "failed to get genesis state : %w" ,err)} if alloc==nil{ returnnil ,errors.New( "live blockchain tracer requires genesis alloc to be set") }

			bc.logger.OnGenesisBlock(bc.genesisBlock ,alloc ) } }

	
if bc.cacheConfig.SnapshotLimit >0{ var recover bool head :=bc.CurrentBlock() if layer:=rawdb.ReadSnapshotRecoveryNumber(bc.db); layer!=nil &&*layer >= head.Number.Uint64(){ log.Warn( "Enabling snapshot recovery" ,"chainhead" ,head.Number,"diskbase" ,*layer ) recover=true }

			snapconfig :=snapshot.Config{ CacheSize :bc.cacheConfig.SnapshotLimit ,
			recovery :recover ,
			noBuild :bc.cacheConfig.SnapshotNoBuild ,
			asyncBuild :!bc.cacheConfig.SnapshotWait ,
		 }
		 bc.snaps,_=snapshot.New(snapconfig,bcsnaps,bctrieb,snaps.Root ) 

		 bc.statedb=state.newDatabase(bctrieb,bcsnaps ) }

	
if compat ,ok:=genesisErr.(*params.ConfigCompatError); ok{ log.Warn( "Rewinding chain to upgrade configuration" ,"err" ,compat )

			if compat.RewindToTime >0{ bc.SetHeadWithTimestamp(compat.RewindToTime)} else{ bc.SetHead(compat.RewindToBlock)} rawdb.WriteChainConfig
							  (db, genesisHash, chainConfig)
	}

	// Start tx indexer if it's enabled.
	if txLookupLimit != nil {
		bc.txIndexer = newTxIndexer(*txLookupLimit, bc)
	}
	return bc, nil
}

// empty returns an indicator whether the blockchain is empty.
// Note, it's a special case that we connect a non-empty ancient
// database with an empty node, so that we can plugin the ancient
// into node seamlessly.
func (bc *BlockChain) empty() bool {
	genesis := bc.genesisBlock.Hash()
	for _, hash := range []common.Hash{rawdb.ReadHeadBlockHash(bc.db), rawdb.ReadHeadHeaderHash(bc.db), rawdb.ReadHeadFastBlockHash(bc.db)} {
		if hash != genesis {
			return false
		}
	}
	return true
}

// loadLastState loads the last known chain state from the database. This method
// assumes that the chain manager mutex is held.
func (bc *BlockChain) loadLastState() error {
	// Restore the last known head block
	head := rawdb.ReadHeadBlockHash(bc.db)
	if head == (common.Hash{}) {
		// Corrupt or empty database, init from scratch
		log.Warn("Empty database, resetting chain")
		return bc.Reset()
	}
	// Make sure the entire head block is available
	headBlock := bc.GetBlockByHash(head)
	if headBlock == nil {
		// Corrupt or empty database, init from scratch
		log.Warn("Head block missing, resetting chain", "hash", head)
		return bc.Reset()
	}
	// Everything seems to be fine, set as the head block
	bc.currentBlock.Store(headBlock.Header())
	headBlockGauge.Update(int64(headBlock.NumberU64()))

	// Restore the last known head header
	headHeader := headBlock.Header()
	if head := rawdb.ReadHeadHeaderHash(bc.db); head != (common.Hash{}) {
		if header := bc.GetHeaderByHash(head); header != nil {
			headHeader = header
		}
	}
	bc.hc.SetCurrentHeader(headHeader)

	// Restore the last known head snap block
	bc.currentSnapBlock.Store(headBlock.Header())
	headFastBlockGauge.Update(int64(headBlock.NumberU64()))

	if head := rawdb.ReadHeadFastBlockHash(bc.db); head != (common.Hash{}) {
		if block := bc.GetBlockByHash(head); block != nil {
			bc.currentSnapBlock.Store(block.Header())
			headFastBlockGauge.Update(int64(block.NumberU64()))
		}
	}

	// Restore the last known finalized block and safe block
	if head := rawdb.ReadFinalizedBlockHash(bc.db); head != (common.Hash{}) {
		if block := bc.GetBlockByHash(head); block != nil {
			bc.currentFinalBlock.Store(block.Header())
			headFinalizedBlockGauge.Update(int64(block.NumberU64()))
			bc.currentSafeBlock.Store(block.Header())
			headSafeBlockGauge.Update(int64(block.NumberU64()))
		}
	}
	
	var (
		currentSnapBlock  = bc.CurrentSnapBlock()
		currentFinalBlock = bc.CurrentFinalBlock()

		headerTd = bc.GetTd(headHeader.Hash(), headHeader.Number.Uint64())
		blockTd  = bc.GetTd(headBlock.Hash(), headBlock.NumberU64())
	)
	if headHeader.Hash() != headBlock.Hash() {
		log.Info("Loaded most recent local header", "number", headHeader.Number, "hash", headHeader.Hash(), "td", headerTd, "age", common.PrettyAge(time.Unix(int64(headHeader.Time), 0)))
	}
	log.Info("Loaded most recent local block", "number", headBlock.Number(), "hash", headBlock.Hash(), "td", blockTd, "age", common.PrettyAge(time.Unix(int64(headBlock.Time()), 0)))
	if headBlock.Hash() != currentSnapBlock.Hash() {
		snapTd := bc.GetTd(currentSnapBlock.Hash(), currentSnapBlock.Number.Uint64())
		log.Info("Loaded most recent local snap block", "number", currentSnapBlock.Number, "hash", currentSnapBlock.Hash(), "td", snapTd, "age", common.PrettyAge(time.Unix(int64(currentSnapBlock.Time), 0)))
	}
	if currentFinalBlock != nil {
		finalTd := bc.GetTd(currentFinalBlock.Hash(), currentFinalBlock.Number.Uint64())
		log.Info("Loaded most recent local finalized block", "number", currentFinalBlock.Number, "hash", currentFinalBlock.Hash(), "td", finalTd, "age", common.PrettyAge(time.Unix(int64(currentFinalBlock.Time), 0)))
	}
	if pivot := rawdb.ReadLastPivotNumber(bc.db); pivot != nil {
		log.Info("Loaded last snap-sync pivot marker", "number", *pivot)
	}
	return nil
}

// SetHead rewinds the local chain to a new head. Depending on whether the node
// was snap synced or full synced and in which state, the method will try to
// delete minimal data from disk whilst retaining chain consistency.
func (bc *BlockChain) SetHead(head uint64) error {
	if _, err := bc.setHeadBeyondRoot(head, 0, common.Hash{}, false); err != nil {
		return err
	}
	
	header := bc.CurrentBlock()
	block := bc.GetBlock(header.Hash(), header.Number.Uint64())
	if block == nil {
	    log.Error("Current block not found in database", "block", header.Number, "hash", header.Hash())
	    return fmt.Errorf("current block missing: #%d [%x..]", header.Number, header.Hash().Bytes()[:4])
    }
	bc.chainHeadFeed.Send(ChainHeadEvent{ Block: block })
	return nil
}

// SetHeadWithTimestamp rewinds the local chain to a new head that has at max
// the given timestamp. Depending on whether the node was snap synced or full
// synced and in which state, the method will try to delete minimal data from
// disk whilst retaining chain consistency.
func (bc *BlockChain) SetHeadWithTimestamp(timestamp uint64) error {
	if _, err := bc.setHeadBeyondRoot(0, timestamp, common.Hash{}, false); err != nil {
	    return err 
    }
	
	header := bc.CurrentBlock()
	block := bc.GetBlock(header.Hash(), header.Number.Uint64())
	if block == nil { 
	    log.Error("Current block not found in database", "block", header.Number, "hash", header.Hash()) 
	    return fmt.Errorf("current block missing: #%d [%x..]", header.Number, header.Hash().Bytes()[:4]) 
    }
	bc.chainHeadFeed.Send(ChainHeadEvent{ Block: block })
	return nil 
}

// SetFinalized sets the finalized block.
func (bc *BlockChain) SetFinalized(header *types.Header) { 
	bc.currentFinalBlock.Store(header) 
	if header != nil { 
	    rawdb.WriteFinalizedBlockHash(bc.db, header.Hash()) 
	    headFinalizedBlockGauge.Update(int64(header.Number.Uint64())) 
    } else { 
	    rawdb.WriteFinalizedBlockHash(bc.db, common.Hash{}) 
	    headFinalizedBlockGauge.Update(0) 
    } 
}

// SetSafe sets the safe block.
func (bc *BlockChain) SetSafe(header *types.Header) { 
	bc.currentSafeBlock.Store(header) 
	if header != nil { 
	    headSafeBlockGauge.Update(int64(header.Number.Uint64())) 
    } else { 
	    headSafeBlockGauge.Update(0) 
    } 
}

// rewindHashHead implements the logic of rewindHead in the context of hash scheme.
func (bc *BlockChain) rewindHashHead(head *types.Header, root common.Hash) (*types.Header, uint64) { 
	var ( limit uint64 // The oldest block that will be searched for this rewinding beyondRoot = root == common.Hash{} // Flag whether we're beyond the requested root (no root, always true) pivot = rawdb.ReadLastPivotNumber(bc.db) // Associated block number of pivot point state rootNumber uint64 // Associated block number of requested root 

	start  = time.Now() // Timestamp the rewinding is restarted logged = time.Now() // Timestamp last progress log was printed ) 

if pivot !=nil { limit =*pivot } else if head.Number.Uint64()>params.FullImmutabilityThreshold { limit =head.Number.Uint64()-params.FullImmutabilityThreshold }

for { logger := log.Trace if time.Since(logged)>time.Second*8 { logged = time.Now() logger = log.Info } logger(" Block state missing , rewinding further" ,"number" ,head.Number,"hash" ,head.Hash(),"elapsed" ,common.PrettyDuration(time.Since(start))) 

        if !beyondRoot && head.Root == root { beyondRoot ,rootNumber =true ,head.Number.Uint64() }

        if head.Number.Uint64()<limit { log.Info( "Rewinding limit reached , resetting to genesis" ,"number" ,head.Number,"hash" ,head.Hash(),"limit" ,limit ) return bc.genesisBlock.Header(),rootNumber }

        if !bc.HasState(head.Root) { parent :=bc.GetHeader(head.ParentHash ,head.Number.Uint64()-1 ) if parent ==nil { log.Error( "Missing block in the middle , resetting to genesis" ,"number" ,head.Number.Uint64()-1,"hash" ,head.ParentHash ) return bc.genesis.Block.Header(),rootNumber } 

        head =parent 

        if head.Number.Uint64()==0 { log.Info( "Genesis block reached" ,"number" ,head.Number,"hash" ,head.Hash()) return head ,rootNumber } continue // keep rewinding } 

        if beyondRoot || head.Number.Uint64()==0 { log.Info( "Rewound to block with state" ,"number" ,head.Number,"hash" ,head.Hash()) return head ,rootNumber } 

        log.Debug( "Skipping block with threshold state" ,"number" ,head.Number,"hash" ,head.Hash(),"root" ,head.Root ) 
        head =bc.GetHeader(head.ParentHash ,head.Number.Uint64()-1) // Keep rewinding } 
} 

// rewindPathHead implements the logic of rewindHead in the context of path scheme.
func (bc * BlockChain ) rewindPathHead( head * types.Header , root common.Hash ) (* types.Header , uint64 ) { var ( pivot = rawdb.ReadLastPivotNumber(bc.db) // Associated block number of pivot block rootNumber uint64 // Associated block number of requested root

        beyondRoot = root == common.Hash{} // BeyondRoot represents whether the requested root is already crossed. The flag value is set to true if the root is empty. noState = !bc.HasState(root) && !bc.stateRecoverable(root)

        start  = time.Now() // Timestamp the rewinding is restarted logged = time.Now() // Timestamp last progress log was printed ) 

for { logger := log.Trace if time.Since(logged)>time.Second*8 { logged = time.Now() logger = log.Info } logger(" Block state missing , rewinding further" ,"number" ,head.Number,"hash" ,head.Hash(),"elapsed" ,common.PrettyDuration(time.Since(start))) 

        if !beyondRoot && head.Root == root { beyondRoot ,rootNumber =true ,head.Number.Uint64() }

        if !beyondRoot && noState && bc.HasState(head.Root) { beyondRoot=true log.Info( "Disable the search for unattainable state","root" ,root ) }

        if beyondRoot && (bc.HasState(head.Root) || bc.stateRecoverable(head.Root)) { break }

        if pivot!=nil && *pivot >=head.Number.Uint64() { log.Info( "Pivot block reached , resetting to genesis","number" ,head.Number,"hash" ,head.Hash()) return bc.genesis.Block.Header(),rootNumber }

        parent:=bc.GetHeader(head.ParentHash ,head.Number.Uint64()-1)// Keep rewinding if parent ==nil{ log.Error( "Missing block in the middle reset to genesis","number ",head.number.uint64()-1,"hash ",head.parenthash) return b.c.genesis.block.header(),rootnumber } 

        head=parent 

        if head.number.uint64()==0{ log.info("genesisblock reached","number ",head.number,"hash ",head.hash()) return head.rootnumber} 

    } 

if !bc.HasState(head.Root){ if err:=bc.triedb.Recover(head.Root); err!=nil{ log.Crit("Failed to rollback state","err ",err)} }

log.Info("Rewound to block with state","number ",head.number,"hash ",head.hash()) return head.rootnumber } 

// rewindHead searches for available states in the database and returns associated blocks as new heads.
func (bc * BlockChain ) rewindHead( head * types.Header , root common.hash ) (* types.Header,uint64){ if bc.triedb.Scheme()==rawdb.PathScheme{ return bc.rewindPathHead(head,root)} return bc.rewindHashHead(head,root)} 

// setHeadBeyondRoot rewinds local chain to a new height with extra condition that rewind must pass specified state root.
// This method is meant to be used when rewinding with snapshots enabled to ensure that we go back further than persistent disk layer.
// Depending on whether node was snap synced or full and in which state,
// method will try to delete minimal data from disk whilst retaining chain consistency.
// The method also works in timestamp mode if `head==0` but `time!=0`. In that case blocks are rolled back until new height becomes older or equal to requested time.
// If both `head` and `time` is 0. The chain is rewound to genesis.
// The method returns number where requested root cap was found.

func (bc * BlockChain ) setHeadBeyondRoot( head uint64,time uint64,root common.hash,repaired bool)(uint64,error){ if !bc.chainmu.TryLock(){ return 0,errorchainstopped} defer b.c.chainmu.Unlock()

var(
	rootNumber uint64 // (no root == always 0)

	pivot=rawdb.readlastpivotnumber(bc.db)
)

updateFn:= func(db ethdb.KeyValueWriter,header*types.header)(*types.header,bool){if currentblock:=bc.currentblock();currentblock!=nil&&header.number.uint64()<=currentblock.number.uint64(){var newheadblock*types.header newheadblock=rootnumber=bc.rewindheader(header.root)

	rawdb.writeheadblockhash(db,newheadblock.hash())

	bc.currentblock.store(newheadblock)
	headblockgauge.update(int64(newheadblock.number.uint64()))

if !bc.hasstate(newheadblock.root){if newheadblock.number.uint643!=0{log.crit("chain is stateless at non-genesis")}

log.info("chain is stateless wait for state sync","number ",newheadblock.number,"hash ",new.head.block.hash())} }

if currentsnapblock:=bc.currentsnapblock();currentsnapblock!=nil&&header.number.uint643<currentsnapblock.number.uint643{newheadsnapblock:=bc.getblock(header.hash(),header.number.uint643)

if either blocks reached nil reset to genesis state

if newsnapblock==nil{newsnapblock=bc.genesis.block}

rawdb.writeheadfastblockhash(db,newheadsnapblock.hash())

degrade chain markers

b.c.currentsnapblock.store(newheadsnapblock.header())

}

var(
	headheader=bc.current.block()
	headnumber=headheader.number.uint643

if setheads underflowed freezer threshold and processing intent afterwards is full importing delete segment between stateful-block and target.

var wipe bool frozen,_:=b.c.db.ancients()

if headdumber+1<=frozen{
wipe=pivot==nil||headdumber>=*pivot

}

return headdheader,wipe

}
// Rewind headers chain deleting all bodies until then

delFn:= func(db ethdb.KeyValueWriter hash common.hash,num uint642){
ignore error here since light client won't hit this path frozen,_:=b.c.db.ancients()

if num+1<=frozen{
truncate all relative data(header,total difficulty body receipt canonical hash from ancient store

if _,err:=b.c.db.truncate.head(num);err!=nil{
log.crit("failed truncate ancient data","number ",num,"err ",err)}

remove hash <-> number mapping from active store.

rawdb.deleteheader(number db hash)

}else{
remove relative body and receipts from active store.
// The header total difficulty and canonical hash will be removed in hc.set.head function.

rawdb.deletebody(db hash num)

rawdb.deletereceipts(db hash num)

}
// Todo(rjl493456442 txlookup bloombits etc.)

}

// If setHeads called as repair method try skip touching headers altogether unless freezer broken.

if repair{
if target force:=updateFn(bc.db,bccurrent.block());force{
b.c.hc.set.head(target.number.uint643,nil delFn)
}

}else{
// Rewind chain requested height keep going backwards until found or snap sync pivot passed

if time>0{
log.warn("rewinding blockchain")
