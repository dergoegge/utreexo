package bridgenode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/btcsuite/btcd/wire"
	"github.com/mit-dci/utreexo/util"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	dbutil "github.com/syndtr/goleveldb/leveldb/util"
)

// Wire Protocol version
// Some btcd lib requires this as an argument
// Technically the version is 70013 but many btcd
// code is passing 0 on some Deserialization methods
const pver uint32 = 0

// MaxMessagePayload is the maximum bytes a message can be regardless of other
// individual limits imposed by messages themselves.
const MaxMessagePayload = (1024 * 1024 * 32) // 32MB
func countOpenFiles() int64 {
	out, err := exec.Command("/bin/sh", "-c", fmt.Sprintf("lsof -p %v", os.Getpid())).Output()
	if err != nil {
		fmt.Println(err.Error())
	}
	lines := strings.Split(string(out), "\n")
	return int64(len(lines) - 1)
}

// RawHeaderData is used for blk*.dat offsetfile building
// Used for ordering blocks as they aren't stored in order in the blk files.
// Includes 32 bytes of sha256 hash along with other variables
// needed for offsetfile building.
type RawHeaderData struct {
	// CurrentHeaderHash is the double hashed 32 byte header
	CurrentHeaderHash [32]byte
	// Prevhash is the 32 byte previous header included in the 80byte header.
	// Needed for ordering
	Prevhash [32]byte
	// FileNum is the blk*.dat file number
	FileNum [4]byte
	// Offset is where it is in the .dat file.
	Offset [4]byte
	// revblock position
	UndoPos uint32
}

// BlockReader is a wrapper around GetRawBlockFromFile so that the process
// can be made into a goroutine. As long as it's running, it keeps sending
// the entire blocktxs and height to bchan with TxToWrite type.
// It also puts in the proofs.  This will run on the archive server, and the
// data will be sent over the network to the CSN.
func BlockAndRevReader(
	blockChan chan BlockAndRev, dataDir, cOffsetFile string,
	knownBlockHeight chan int32, curHeight int32) {

	var offsetFilePath string

	// If empty string is given, just use the default path
	// If not, then use the custom one given
	if cOffsetFile == "" {
		offsetFilePath = util.OffsetFilePath
	} else {
		offsetFilePath = cOffsetFile
	}

	for {
		maxHeight, ok := <-knownBlockHeight
		if !ok {
			fmt.Println("BlockAndRevReader: knownBlockHeight channel got closed, bye.")
			return
		}

		fmt.Println("BlockAndRevReader: reading blocks until", maxHeight,
			", current height:", curHeight, "open files", countOpenFiles())

		for curHeight < maxHeight {
			if curHeight%100 == 0 {
				fmt.Println("BlockAndRevReader:", curHeight)
			}
			blk, rb, err := GetRawBlockFromFile(curHeight, offsetFilePath, dataDir)
			if err != nil {
				fmt.Println(curHeight)
				panic(err)
			}

			bnr := BlockAndRev{Height: curHeight, Blk: blk, Rev: rb}

			blockChan <- bnr
			curHeight++
		}
	}

}

// GetRawBlocksFromFile reads the blocks from the given .dat file and
// returns those blocks.
// Skips the genesis block. If you search for block 0, it will give you
// block 1.
func GetRawBlockFromFile(tipnum int32, offsetFileName string, blockDir string) (
	block wire.MsgBlock, rBlock RevBlock, err error) {
	if tipnum == 0 {
		err = fmt.Errorf("Block 0 is not in blk files or utxo set")
		return
	}
	tipnum--

	var datFile, offset, revOffset uint32

	offsetFile, err := os.Open(offsetFileName)
	if err != nil {
		return
	}
	defer offsetFile.Close() // file always closes

	// offset file consists of 12 bytes per block
	// tipnum * 12 gives us the correct position for that block
	_, err = offsetFile.Seek(int64(12*tipnum), 0)
	if err != nil {
		return
	}

	// Read file and offset for the block
	err = binary.Read(offsetFile, binary.BigEndian, &datFile)
	if err != nil {
		return
	}
	err = binary.Read(offsetFile, binary.BigEndian, &offset)
	if err != nil {
		return
	}
	err = binary.Read(offsetFile, binary.BigEndian, &revOffset)
	if err != nil {
		return
	}

	blockFName := fmt.Sprintf("blk%05d.dat", datFile)
	revFName := fmt.Sprintf("rev%05d.dat", datFile)

	bDir := filepath.Join(blockDir, blockFName)
	rDir := filepath.Join(blockDir, revFName)

	blockFile, err := os.Open(bDir)
	if err != nil {
		return
	}
	defer blockFile.Close() // file always closes

	// +8 skips the 8 bytes of magicbytes and load size
	_, err = blockFile.Seek(int64(offset)+8, 0)
	if err != nil {
		return
	}

	// TODO this is probably expensive. fix
	err = block.Deserialize(blockFile)
	if err != nil {
		return
	}

	revFile, err := os.Open(rDir)
	if err != nil {
		return
	}
	defer revFile.Close() // file always closes

	revFile.Seek(int64(revOffset), 0)
	err = rBlock.Deserialize(revFile)
	if err != nil {
		return
	}

	return
}

// BlockAndRev is a regular block and a rev block stuck together
type BlockAndRev struct {
	Height int32
	Rev    RevBlock
	Blk    wire.MsgBlock
}

/*
 * All types here follow the Bitcoin Core implementation of the
 * Undo blocks. One difference is that all the vectors are replaced
 * with slices. This is just a language difference.
 *
 * Compression/Decompression and VarInt functions are all taken/using
 * btcsuite packages.
 */

// RevBlock is the structure of how a block is stored in the
// rev*.dat file the Bitcoin Core generates
type RevBlock struct {
	Magic [4]byte   // Network magic bytes
	Size  [4]byte   // size of the BlockUndo record
	Txs   []*TxUndo // acutal undo record
	Hash  [32]byte  // 32 byte double sha256 hash of the block
}

// TxUndo contains the TxInUndo records.
// see github.com/bitcoin/bitcoin/src/undo.h
type TxUndo struct {
	TxIn []*TxInUndo
}

// TxInUndo is the stucture of the undo transaction
// Eveything is uncompressed here
// see github.com/bitcoin/bitcoin/src/undo.h
type TxInUndo struct {
	Height int32

	// Version of the original tx that created this tx
	Varint uint64

	// scriptPubKey of the spent UTXO
	PKScript []byte

	// Value of the spent UTXO
	Amount int64

	// Whether if the TxInUndo is a coinbase or not
	// Not actually included in the rev*.dat files
	Coinbase bool
}

// Deserialize takes a reader and reads a single block
// Only initializes the Block var in RevBlock
func (rb *RevBlock) Deserialize(r io.Reader) error {
	txCount, err := wire.ReadVarInt(r, pver)
	if err != nil {
		return err
	}

	for i := uint64(0); i < txCount; i++ {
		var tx TxUndo
		err := tx.Deserialize(r)
		if err != nil {
			return err
		}
		rb.Txs = append(rb.Txs, &tx)
	}
	return nil
}

// Deserialize takes a reader and reads all the TxUndo data
func (tx *TxUndo) Deserialize(r io.Reader) error {

	// Read the Variable Integer
	count, err := wire.ReadVarInt(r, pver)
	if err != nil {
		return err
	}

	for i := uint64(0); i < count; i++ {
		var in TxInUndo
		err := readTxInUndo(r, &in)
		if err != nil {
			return err
		}
		tx.TxIn = append(tx.TxIn, &in)
	}
	return nil
}

// readTxInUndo reads all the TxInUndo from the reader to the passed in txInUndo
// variable
func readTxInUndo(r io.Reader, ti *TxInUndo) error {
	// nCode is how height is saved to the rev files
	nCode, _ := deserializeVLQ(r)
	ti.Height = int32(nCode / 2) // Height is saved as actual height * 2
	ti.Coinbase = nCode&1 == 1   // Coinbase is odd. Saved as height * 2 + 1

	// Only TxInUndos that have the height greater than 0
	// Has varint that isn't 0. see
	// github.com/bitcoin/bitcoin/blob/9cc7eba1b5651195c05473004c00021fe3856f30/src/undo.h#L42
	// if ti.Height > 0 {
	varint, err := wire.ReadVarInt(r, pver)
	if err != nil {
		return err
	}

	if varint != 0 {
		return fmt.Errorf("varint is %d", varint)
	}
	ti.Varint = varint

	amount, _ := deserializeVLQ(r)
	ti.Amount = decompressTxOutAmount(amount)

	ti.PKScript = decompressScript(r)
	if ti.PKScript == nil {
		return fmt.Errorf("nil pkscript on h %d, pks %x\n", ti.Height, ti.PKScript)

	}

	return nil
}

// OpenIndexFile returns the db with only read only option enabled
func OpenIndexFile(indexDir string) (*leveldb.DB, error) {
	// Read-only and no compression on
	// Bitcoin Core uses uncompressed leveldb. If that db is
	// opened EVEN ONCE, with compression on, the user will
	// have to re-index (takes hours, maybe days)
	o := opt.Options{ReadOnly: true, Compression: opt.NoCompression}
	lvdb, err := leveldb.OpenFile(indexDir, &o)
	if err != nil {
		return nil, fmt.Errorf("can't open %s\n", indexDir)
	}

	return lvdb, nil
}

// CBlockFileIndex is a reimplementation of the Bitcoin Core
// class CBlockFileIndex
type CBlockFileIndex struct {
	Version int32  // nVersion info of the block
	Height  int32  // Height of the block
	Status  int32  // validation status of the block in Bitcoin Core
	TxCount int32  // tx count in the block
	File    int32  // file num
	DataPos uint32 // blk*.dat file offset
	UndoPos uint32 // rev*.dat file offset
}

const (
	// BlockHaveUndo indicates that undo data is available in rev*.dat
	BlockHaveUndo int32 = 16
)

// BufferDB buffers the leveldb key values into map in memory
func BufferDB(lvdb *leveldb.DB) map[[32]byte]uint32 {
	bufDB := make(map[[32]byte]uint32)
	var header [32]byte

	iter := lvdb.NewIterator(dbutil.BytesPrefix([]byte{0x62}), nil)
	for iter.Next() {
		copy(header[:], iter.Key()[1:])
		cbIdx := ReadCBlockFileIndex(bytes.NewReader(iter.Value()))

		if cbIdx.Status&BlockHaveUndo > 0 {
			// only write the undopos into the map if a undo position is available
			bufDB[header] = cbIdx.UndoPos
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		panic(err)
	}

	return bufDB
}

func ReadCBlockFileIndex(r io.ReadSeeker) (cbIdx CBlockFileIndex) {
	// not sure if nVersion is correct...?
	nVersion, _ := deserializeVLQ(r)
	cbIdx.Version = int32(nVersion)

	nHeight, _ := deserializeVLQ(r)
	cbIdx.Height = int32(nHeight)

	// nStatus is incorrect but everything else correct. Probably reading this wrong
	nStatus, _ := deserializeVLQ(r)
	cbIdx.Status = int32(nStatus)

	nTx, _ := deserializeVLQ(r)
	cbIdx.TxCount = int32(nTx)

	nFile, _ := deserializeVLQ(r)
	cbIdx.File = int32(nFile)

	nDataPos, _ := deserializeVLQ(r)
	cbIdx.DataPos = uint32(nDataPos)

	nUndoPos, _ := deserializeVLQ(r)
	cbIdx.UndoPos = uint32(nUndoPos)

	// Need to seek 3 bytes if you're fetching the actual
	// header information. Not sure why it's needed but there's
	// no documentation to be found on the Bitcoin Core side
	// r.Seek(3, 1)

	return cbIdx
}
