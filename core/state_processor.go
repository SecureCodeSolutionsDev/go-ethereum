// Copyright 2015 The go-ethereum Authors
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

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// StateProcessor is responsible for transitioning state from one point to another.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	chain  *HeaderChain        // Canonical header chain
}

// NewStateProcessor initializes a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, chain *HeaderChain) *StateProcessor {
	return &StateProcessor{
		config: config,
		chain:  chain,
	}
}

// Process processes state changes according to Ethereum rules.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (*ProcessResult, error) {
	var (
		receipts    types.Receipts
		usedGas     uint64
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		gp          = new(GasPool).AddGas(block.GasLimit())
	)

	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	context := NewEVMBlockContext(header, p.chain, nil)
	signer := types.MakeSigner(p.config, header.Number, header.Time)
	vmenv := vm.NewEVM(context, vm.TxContext{}, statedb, p.config, cfg)

	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		ProcessBeaconBlockRoot(*beaconRoot, vmenv, statedb)
	}
	if p.config.IsPrague(block.Number(), block.Time()) {
		ProcessParentBlockHash(block.ParentHash(), vmenv, statedb)
	}

	for i, tx := range block.Transactions() {
		msg, err := TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		statedb.SetTxContext(tx.Hash(), i)

        // Apply transaction and handle errors appropriately.
        receipt, err := ApplyTransactionWithEVM(msg, p.config, gp, statedb,
			blockNumber, blockHash, tx, &usedGas, vmenv)
        if err != nil {
            return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
        }

        receipts = append(receipts, receipt)
        allLogs = append(allLogs, receipt.Logs...)
    }

	var requests [][]byte
	if p.config.IsPrague(block.Number(), block.Time()) {
        depositRequests, err := ParseDepositLogs(allLogs, p.config)
        if err != nil {
            return nil, err
        }
        requests = append(requests, depositRequests)
    }

	p.chain.engine.Finalize(p.chain, header, statedb, block.Body())

	return &ProcessResult{
        Receipts: receipts,
        Logs:     allLogs,
        GasUsed:  usedGas,
        Requests: requests,
    }, nil
}

// ApplyTransactionWithEVM applies a transaction to the state database using an existing EVM instance.
func ApplyTransactionWithEVM(msg *Message,
	config *params.ChainConfig,
	gp *GasPool,
	statedb *state.StateDB,
	blockNumber *big.Int,
	blockHash common.Hash,
	tx *types.Transaction,
    usedGas *uint64,
	evm *vm.EVM) (*types.Receipt, error) {

	if evm.Config.Tracer != nil && evm.Config.Tracer.OnTxStart != nil {
        evm.Config.Tracer.OnTxStart(evm.GetVMContext(), tx, msg.From)
        defer func() {
            if evm.Config.Tracer.OnTxEnd != nil {
                evm.Config.Tracer.OnTxEnd(nil /* receipt */, nil /* err */)
            }
        }()
    }

	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
        return nil, err
    }

	var root []byte
	if config.IsByzantium(blockNumber) {
        statedb.Finalise(true)
    } else {
        root = statedb.IntermediateRoot(config.IsEIP158(blockNumber)).Bytes()
    }
    
    *usedGas += result.UsedGas

	return MakeReceipt(evm, result, statedb,
        blockNumber,
        blockHash,
        tx,
        *usedGas,
        root), nil
}

// MakeReceipt generates a receipt for a transaction given its execution result.
func MakeReceipt(evm *vm.EVM,
	result *ExecutionResult,
	statedb *state.StateDB,
	blockNumber *big.Int,
	blockHash common.Hash,
	tx *types.Transaction,
    usedGas uint64,
	root []byte) *types.Receipt {

    receipt := &types.Receipt{
        Type:              tx.Type(),
        PostState:         root,
        CumulativeGasUsed: usedGas,
    }
    
    if result.Failed() {
        receipt.Status = types.ReceiptStatusFailed
    } else {
        receipt.Status = types.ReceiptStatusSuccessful
    }
    
    receipt.TxHash = tx.Hash()
    receipt.GasUsed = result.UsedGas

    if tx.Type() == types.BlobTxType {
        receipt.BlobGasUsed = uint64(len(tx.BlobHashes()) * params.BlobTxBlobGasPerBlob)
        receipt.BlobGasPrice = evm.Context.BlobBaseFee
    }

    if tx.To() == nil { // Contract creation transaction
        receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
    }

    if statedb.GetTrie().IsVerkle() {
        statedb.AccessEvents().Merge(evm.AccessEvents)
    }

    receipt.Logs = statedb.GetLogs(tx.Hash(), blockNumber.Uint64(), blockHash)
    receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
    receipt.BlockHash = blockHash
    receipt.BlockNumber = blockNumber
    receipt.TransactionIndex = uint(statedb.TxIndex())
    
	return receipt
}

// ApplyTransaction attempts to apply a transaction to the given state database.
// It returns the receipt for the transaction and an error if it failed.
func ApplyTransaction(config *params.ChainConfig,
	bc ChainContext,
	author *common.Address,
	gp *GasPool,
	statedb *state.StateDB,
	header *types.Header,
	tx *types.Transaction,
	usedGas *uint64,
	cfg vm.Config) (*types.Receipt, error) {

	msg, err := TransactionToMessage(tx, types.MakeSigner(config, header.Number, header.Time), header.BaseFee)
	if err != nil {
	    return nil, err
	}

	blockContext := NewEVMBlockContext(header, bc, author)
	txContext := NewEVMTxContext(msg)
	vmenv := vm.NewEVM(blockContext, txContext, statedb, config, cfg)

	return ApplyTransactionWithEVM(msg, config, gp,
	    statedb,
	    header.Number,
	    header.Hash(),
	    tx,
	    usedGas,
	    vmenv)
}

// ProcessBeaconBlockRoot applies EIP-4788 system call to the beacon block root contract.
func ProcessBeaconBlockRoot(beaconRoot common.Hash,
	vmenv *vm.EVM,
	statedb *state.StateDB) {

	if tracer := vmenv.Config.Tracer; tracer != nil {
	    if tracer.OnSystemCallStart != nil {
	        tracer.OnSystemCallStart()
	    }
	    if tracer.OnSystemCallEnd != nil {
	        defer tracer.OnSystemCallEnd()
	    }
	}

	msg := &Message{
	    From:      params.SystemAddress,
	    GasLimit:  30_000_000,
	    GasPrice:  common.Big0,
	    GasFeeCap: common.Big0,
	    GasTipCap: common.Big0,
	    To:        &params.BeaconRootsAddress,
	    Data:      beaconRoot[:],
	}
	vmenv.Reset(NewEVMTxContext(msg), statedb)
	statedb.AddAddressToAccessList(params.BeaconRootsAddress)

	_, _, _ = vmenv.Call(vm.AccountRef(msg.From), *msg.To, msg.Data, 30_000_000, common.U2560)
	statedb.Finalise(true)
}

// ProcessParentBlockHash stores the parent block hash in history storage contract as per EIP-2935.
func ProcessParentBlockHash(prevHash common.Hash,
	vmenv *vm.EVM,
	statedb *state.StateDB) {

	if tracer := vmenv.Config.Tracer; tracer != nil {
	    if tracer.OnSystemCallStart != nil {
	        tracer.OnSystemCallStart()
	    }
	    if tracer.OnSystemCallEnd != nil {
	        defer tracer.OnSystemCallEnd()
	    }
	}

	msg := &Message{
	    From:      params.SystemAddress,
	    GasLimit:  30_000_000,
	    GasPrice:  common.Big0,
	    GasFeeCap: common.Big0,
	    GasTipCap: common.Big0,
	    To:        &params.HistoryStorageAddress,
	    Data:      prevHash.Bytes(),
	}
	vmenv.Reset(NewEVMTxContext(msg), statedb)
	statedb.AddAddressToAccessList(params.HistoryStorageAddress)

	_, _, _ = vmenv.Call(vm.AccountRef(msg.From), *msg.To, msg.Data, 30_000_000, common.U2560)
	statedb.Finalise(true)
}

// ParseDepositLogs extracts EIP-6110 deposit values from logs emitted by BeaconDepositContract.
func ParseDepositLogs(logs []*types.Log,
	config *params.ChainConfig) ([]byte,error) {

	deposits := make([]byte ,1) // note: first byte is 0x00 (== deposit request type)

	for _, log := range logs {
	    if log.Address == config.DepositContractAddress {
	        request ,err := types.DepositLogToRequest(log.Data )
	        if err !=nil{
	            return nil , fmt.Errorf("unable to parse deposit data : %v",err )
	        }
	        deposits= append(deposits ,request...)
	    }
	}
	return deposits ,nil
}
