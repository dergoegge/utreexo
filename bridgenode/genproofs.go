package bridgenode

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/mit-dci/utreexo/accumulator"
	"github.com/mit-dci/utreexo/util"
	"github.com/mit-dci/utreexo/util/ttl"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// build the bridge node / proofs
func BuildProofs(
	net wire.BitcoinNet, ttlpath, offsetfile string, sig chan bool) error {

	// Channel to alert the tell the main loop it's ok to exit
	done := make(chan bool, 1)

	// Waitgroup to alert stopBuildProofs() that revoffet and offset has
	// been finished
	offsetFinished := make(chan bool, 1)

	// Channel for stopBuildProofs() to wait
	finish := make(chan bool, 1)

	// Handle user interruptions
	go stopBuildProofs(sig, offsetFinished, done, finish)

	// If given the option testnet=true, check if the blk00000.dat file
	// in the directory is a testnet file. Vise-versa for mainnet
	util.CheckNet(net)

	// Creates all the directories needed for bridgenode
	util.MakePaths()

	// Init forest and variables. Resumes if the data directory exists
	forest, height, knownTipHeight, err :=
		initBridgeNodeState(net, offsetFinished)
	if err != nil {
		panic(err)
	}

	// Open leveldb
	o := new(opt.Options)
	o.CompactionTableSizeMultiplier = 8
	lvdb, err := leveldb.OpenFile(ttlpath, o)
	if err != nil {
		panic(err)
	}
	defer lvdb.Close()

	// For ttl value writing
	var batchwg sync.WaitGroup
	batchan := make(chan *leveldb.Batch, 10)

	// Start 16 workers. Just an arbitrary number
	for j := 0; j < 16; j++ {
		go ttl.DbWorker(batchan, lvdb, &batchwg)
	}

	// To send/receive blocks from blockreader()
	blockAndRevReadQueue := make(chan util.BlockAndRev, 10)

	// Reads block asynchronously from .dat files
	// Reads util the lastIndexOffsetHeight
	go util.BlockAndRevReader(blockAndRevReadQueue,
		knownTipHeight, height)
	proofChan := make(chan []byte, 10)
	var fileWait sync.WaitGroup
	go proofWriterWorker(proofChan, &fileWait)

	fmt.Println("Building Proofs and ttldb...")

	var stop bool // bool for stopping the main loop

	for ; height != knownTipHeight && stop != true; height++ {

		// Receive txs from the asynchronous blk*.dat reader
		bnr := <-blockAndRevReadQueue

		// Writes the ttl values for each tx to leveldb
		ttl.WriteBlock(bnr, batchan, &batchwg)

		// Get the add and remove data needed from the block & undo block
		blockAdds, delLeaves, err := blockToAddDel(bnr)
		if err != nil {
			return err
		}

		// use the accumulator to get inclusion proofs, and produce a block
		// proof with all data needed to verify the block
		ud, err := genUData(delLeaves, forest, bnr.Height)
		if err != nil {
			return err
		}

		// convert UData struct to bytes
		b := ud.ToBytes()

		// Add to WaitGroup and send data to channel to be written
		// to disk
		fileWait.Add(1)
		proofChan <- b

		ud.AccProof.SortTargets()

		// fmt.Printf("h %d adds %d targets %d\n",
		// 	height, len(blockAdds), len(ud.AccProof.Targets))

		// TODO: Don't ignore undoblock
		// Modifies the forest with the given TXINs and TXOUTs
		_, err = forest.Modify(blockAdds, ud.AccProof.Targets)
		if err != nil {
			return err
		}

		if bnr.Height%10000 == 0 {
			fmt.Println("On block :", bnr.Height+1)
		}

		// Check if stopSig is no longer false
		// stop = true makes the loop exit
		select {
		case stop = <-done: // receives true from stopBuildProofs()
		default:
		}
	}

	// wait until dbWorker() has written to the ttldb file
	// allows leveldb to close gracefully
	batchwg.Wait()

	// Wait for the file workers to finish
	fileWait.Wait()

	// Save the current state so genproofs can be resumed
	err = saveBridgeNodeData(forest, height)
	if err != nil {
		panic(err)
	}

	fmt.Println("Done writing")

	// Tell stopBuildProofs that it's ok to exit
	finish <- true
	return nil

}

// genBlockProof calls forest.ProveBatch with the hash data to get a batched
// inclusion proof from the accumulator. It then adds on the utxo leaf data,
// to create a block proof which both proves inclusion and gives all utxo data
// needed for transaction verification.
func genUData(delLeaves []util.LeafData, f *accumulator.Forest, height int32) (
	ud util.UData, err error) {

	ud.UtxoData = delLeaves
	// make slice of hashes from leafdata
	delHashes := make([]accumulator.Hash, len(ud.UtxoData))
	for i, _ := range ud.UtxoData {
		delHashes[i] = ud.UtxoData[i].LeafHash()
		// fmt.Printf("del %s -> %x\n",
		// ud.UtxoData[i].Outpoint.String(), delHashes[i][:4])
	}
	// generate block proof. Errors if the tx cannot be proven
	// Should never error out with genproofs as it takes
	// blk*.dat files which have already been vetted by Bitcoin Core
	ud.AccProof, err = f.ProveBatch(delHashes)
	if err != nil {
		err = fmt.Errorf("genUData failed at block %d %s %s",
			height, f.Stats(), err.Error())
		return
	}

	if len(ud.AccProof.Targets) != len(delLeaves) {
		err = fmt.Errorf("genUData %d targets but %d leafData",
			len(ud.AccProof.Targets), len(delLeaves))
		return
	}

	// fmt.Printf(batchProof.ToString())
	// Optional Sanity check. Should never fail.

	// unsort := make([]uint64, len(ud.AccProof.Targets))
	// copy(unsort, ud.AccProof.Targets)
	// ud.AccProof.SortTargets()
	// ok := f.VerifyBatchProof(ud.AccProof)
	// if !ok {
	// 	return ud, fmt.Errorf("VerifyBatchProof failed at block %d", height)
	// }
	// ud.AccProof.Targets = unsort

	// also optional, no reason to do this other than bug checking

	// if !ud.Verify(f.ReconstructStats()) {
	// 	err = fmt.Errorf("height %d LeafData / Proof mismatch", height)
	// 	return
	// }
	return
}

// genAddDel is a wrapper around genAdds and genDels. It calls those both and
// throws out all the same block spends.
// It's a little redundant to give back both delLeaves and delHashes, since the
// latter is just the hash of the former, but if we only return delLeaves we
// end up hashing them twice which could slow things down.
func blockToAddDel(bnr util.BlockAndRev) (blockAdds []accumulator.Leaf,
	delLeaves []util.LeafData, err error) {

	inskip, outskip := util.DedupeBlock(&bnr.Blk)
	// fmt.Printf("inskip %v outskip %v\n", inskip, outskip)
	delLeaves, err = blockNRevToDelLeaves(bnr, inskip)
	if err != nil {
		return
	}

	// this is bridgenode, so don't need to deal with memorable leaves
	blockAdds = util.BlockToAddLeaves(bnr.Blk, nil, outskip, bnr.Height)

	return
}

// genDels generates txs to be deleted from the Utreexo forest. These are TxIns
func blockNRevToDelLeaves(bnr util.BlockAndRev, skiplist []uint32) (
	delLeaves []util.LeafData, err error) {

	// make sure same number of txs and rev txs (minus coinbase)
	if len(bnr.Blk.Transactions)-1 != len(bnr.Rev.Txs) {
		err = fmt.Errorf("genDels block %d %d txs but %d rev txs",
			bnr.Height, len(bnr.Blk.Transactions), len(bnr.Rev.Txs))
		return
	}

	var blockInIdx uint32
	for txinblock, tx := range bnr.Blk.Transactions {
		if txinblock == 0 {
			blockInIdx++ // coinbase tx always has 1 input
			continue
		}
		txinblock--
		// make sure there's the same number of txins
		if len(tx.TxIn) != len(bnr.Rev.Txs[txinblock].TxIn) {
			err = fmt.Errorf("genDels block %d tx %d has %d inputs but %d rev entries",
				bnr.Height, txinblock+1,
				len(tx.TxIn), len(bnr.Rev.Txs[txinblock].TxIn))
			return
		}
		// loop through inputs
		for i, txin := range tx.TxIn {
			// check if on skiplist.  If so, don't make leaf
			if len(skiplist) > 0 && skiplist[0] == blockInIdx {
				// fmt.Printf("skip %s\n", txin.PreviousOutPoint.String())
				skiplist = skiplist[1:]
				blockInIdx++
				continue
			}

			// build leaf
			var l util.LeafData

			l.Outpoint = txin.PreviousOutPoint
			l.Height = bnr.Rev.Txs[txinblock].TxIn[i].Height
			l.Coinbase = bnr.Rev.Txs[txinblock].TxIn[i].Coinbase
			// TODO get blockhash from headers here -- empty for now
			// l.BlockHash = getBlockHashByHeight(l.CbHeight >> 1)
			l.Amt = bnr.Rev.Txs[txinblock].TxIn[i].Amount
			l.PkScript = bnr.Rev.Txs[txinblock].TxIn[i].PKScript
			delLeaves = append(delLeaves, l)
			blockInIdx++
		}
	}
	return
}

// stopBuildProofs listens for the signal from the OS and initiates an exit squence
func stopBuildProofs(
	sig, offsetfinished, done, finish chan bool) {

	// Listen for SIGINT, SIGQUIT, SIGTERM
	<-sig

	// Sometimes there are bugs that make the program run forver.
	// Utreexo binary should never take more than 10 seconds to exit
	go func() {
		time.Sleep(10 * time.Second)
		fmt.Println("Program timed out. Force quitting. Data likely corrupted")
		os.Exit(1)
	}()

	// Tell the user that the sig is received
	fmt.Println("User exit signal received. Exiting...")

	select {
	// If offsetfile is there or was built, don't remove it
	case <-offsetfinished:
		select {
		default:
			done <- true
		}
	// If nothing is received, delete offsetfile and other directories
	// Don't wait for done channel from the main BuildProofs() for loop
	default:
		select {
		default:
			fmt.Println("offsetfile incomplete, removing...")
			// May not work sometimes.
			err := os.RemoveAll(util.OffsetDirPath)
			if err != nil {
				fmt.Println("ERR. offsetdata/ directory not removed. Please manually remove it.")
			}
			err = os.RemoveAll(util.RevOffsetDirPath)
			if err != nil {
				fmt.Println("ERR. revdata/ directory not removed. Please manually remove it.")
			}
			fmt.Println("Exiting...")
			os.Exit(0)
		}
	}

	// Wait until BuildProofs() or buildOffsetFile() says it's ok to exit
	<-finish
	os.Exit(0)
}