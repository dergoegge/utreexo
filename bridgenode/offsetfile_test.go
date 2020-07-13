package bridgenode

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/mit-dci/utreexo/util"
	"github.com/syndtr/goleveldb/leveldb"
)

func BenchmarkBuildOffsetFile(b *testing.B) {
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir) // clean up. Always runs

	// grab the datadir for this system
	// use testnet3
	testnetDataDir := filepath.Join("/Users/niklas/.bitcoin", "testnet3/blocks")
	tmpOffsetFile := filepath.Join(tmpDir, "offsetfile")
	tmpLastOffsetHeightFile := filepath.Join(tmpDir, "loffsetfile")

	hash, err := util.GenHashForNet(chaincfg.TestNet3Params)
	if err != nil {
		b.Fatal(err)
	}
	fmt.Println("creating offestfile...")
	offsetfile, err := NewOffsetFile(testnetDataDir, tmpOffsetFile,
		tmpLastOffsetHeightFile, *hash)
	if err != nil {
		b.Fatal(err)
	}
	defer offsetfile.Close()
	fmt.Println("building offsetfile...")
	newBlocks := make(chan bool)
	haltReq := make(chan bool)
	haltAccepted := make(chan bool)
	lastIndexed := offsetfile.Build(newBlocks, haltReq, haltAccepted)
	newBlocks <- true

	// wait for offsetfile to finish
	for {
		if <-lastIndexed > 1500000 {
			fmt.Println("Done")
			break
		}
	}
}

func TestRandom(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir) // clean up. Always runs

	// grab the datadir for this system
	// use testnet3
	testnetDataDir := filepath.Join("/Volumes/bitcoindata/.bitcoin/", "testnet3/blocks")
	// grab testnet3 hash
	testnetHash, err := util.GenHashForNet(chaincfg.TestNet3Params)
	if err != nil {
		t.Fatal(err)
	}

	tmpOffsetFile := filepath.Join(tmpDir, "offsetfile")
	tmpLastOffsetHeightFile := filepath.Join(tmpDir, "loffsetfile")

	// build offsetfile
	fmt.Println("creating offestfile...")
	offsetfile, err := NewOffsetFile(testnetDataDir, tmpOffsetFile,
		tmpLastOffsetHeightFile, *testnetHash)
	if err != nil {
		t.Fatal(err)
	}
	defer offsetfile.Close()
	fmt.Println("building offsetfile...")
	newBlocks := make(chan bool)
	haltReq := make(chan bool)
	haltAccepted := make(chan bool)
	lastIndexed := offsetfile.Build(newBlocks, haltReq, haltAccepted)
	newBlocks <- true

	// Check that things in the offsetfile are correct
	// 200,000 blocks is prob enough
	for i := int32(1); i < 1750000; i++ { // skip genesis
		fmt.Println(<-lastIndexed)
	}

	fmt.Println("Done")
}

func TestBuildOffsetFile(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir) // clean up. Always runs

	// grab the datadir for this system
	// use testnet3
	testnetDataDir := filepath.Join("/Users/niklas/.bitcoin", "testnet3/blocks")
	// grab testnet3 hash
	testnetHash, err := util.GenHashForNet(chaincfg.TestNet3Params)
	if err != nil {
		t.Fatal(err)
	}

	tmpOffsetFile := filepath.Join(tmpDir, "offsetfile")
	tmpLastOffsetHeightFile := filepath.Join(tmpDir, "loffsetfile")

	// build offsetfile
	fmt.Println("creating offestfile...")
	offsetfile, err := NewOffsetFile(testnetDataDir, tmpOffsetFile,
		tmpLastOffsetHeightFile, *testnetHash)
	if err != nil {
		t.Fatal(err)
	}
	defer offsetfile.Close()
	fmt.Println("building offsetfile...")
	newBlocks := make(chan bool)
	haltReq := make(chan bool)
	haltAccepted := make(chan bool)
	lastIndexed := offsetfile.Build(newBlocks, haltReq, haltAccepted)
	newBlocks <- true
	lvdb, _ := OpenIndexFile(filepath.Join(testnetDataDir, "/index"))
	bnrChan := make(chan BlockAndRev, 10)

	fmt.Println("checking the offestfile created...")

	// Start the reader
	go BlockAndRevReader(bnrChan, testnetDataDir, tmpOffsetFile,
		lastIndexed, 1)

	// Check that things in the offsetfile are correct
	// 200,000 blocks is prob enough
	for i := int32(1); i < 1770000; i++ { // skip genesis
		bnr := <-bnrChan

		cbIdx := GetBlockIndexInfo(bnr.Blk.BlockHash(), lvdb)
		// Check that the height is correct
		if cbIdx.Height != i {
			err := fmt.Errorf(
				"CBlockFileIndex height is: %d but it should be %d",
				cbIdx.Height, i)
			t.Fatal(err)
		}
		// Check that there are same number of txs and rev txs (minus coinbase)
		if len(bnr.Blk.Transactions)-1 != len(bnr.Rev.Txs) {
			err := fmt.Errorf(
				"block height: %d has %d txs but rev block has: %d txs",
				i, len(bnr.Blk.Transactions), len(bnr.Rev.Txs))
			t.Fatal(err)
		}
		if i%20000 == 0 {
			fmt.Println("# of tested blocks: ", i)
		}
	}

	fmt.Println("Done")
}

// GetBlockIndexInfo returns a CBlockFileIndex based on the hash given as a key
func GetBlockIndexInfo(h [32]byte, lvdb *leveldb.DB) CBlockFileIndex {
	// 0x62 is hex representation of ascii 'b' (98), which is used
	// appended to block keys in leveldb
	lookup := append([]byte{0x62}, h[:]...)

	value, err := lvdb.Get(lookup, nil)
	if err == leveldb.ErrClosed { // Handle db closed err
		panic(err)
	}
	// Sometimes there may be a block in blk that is not verified but is just sitting there
	// Warn the user about it but ignore it since it doesn't effect the actual validation
	if err != nil { // all other returned errors are from reading the db
		str := fmt.Errorf("%s WARNING: A block in blk file exists without"+
			"a corresponding rev block location. May be wasting disk space", err)
		fmt.Println(str)
	}

	r := bytes.NewReader(value)
	cbIdx := ReadCBlockFileIndex(r)

	return cbIdx
}
