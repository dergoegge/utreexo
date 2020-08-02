package bridgenode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"

	"github.com/btcsuite/btcd/wire"
	"github.com/mit-dci/utreexo/accumulator"
	"github.com/mit-dci/utreexo/util"
	"github.com/syndtr/goleveldb/leveldb"
)

// blockServer listens on a TCP port for incoming connections, then gives
// ublocks blocks over that connection
func blockServer(endHeight int32, dataDir string, haltRequest,
	haltAccept chan bool, lvdb *leveldb.DB) {
	listenAdr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:8338")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	listener, err := net.ListenTCP("tcp", listenAdr)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	cons := make(chan net.Conn)
	go acceptConnections(listener, cons)

	for {
		select {
		case <-haltRequest:
			listener.Close()
			haltAccept <- true
			close(cons)
			return
		case con := <-cons:
			go serveBlocksWorker(con, endHeight, dataDir, lvdb)
		}
	}
}

func acceptConnections(listener *net.TCPListener, cons chan net.Conn) {
	for {
		select {
		case <-cons:
			// cons got closed, stop accepting new connections
			return
		default:
		}

		con, err := listener.Accept()
		if err != nil {
			fmt.Printf("blockServer accept error: %s\n", err.Error())
			return
		}

		cons <- con
	}
}

// serveBlocksWorker gets height requests from client and sends out the ublock
// for that height
func serveBlocksWorker(c net.Conn, endHeight int32, blockDir string, lvdb *leveldb.DB) {
	defer c.Close()
	fmt.Printf("start serving %s\n", c.RemoteAddr().String())
	var fromHeight, toHeight int32
	var clientState accumulator.Pollard

	var fullProof, reducedProof uint64

	for {
		err := binary.Read(c, binary.BigEndian, &fromHeight)
		if err != nil {
			fmt.Printf("pushBlocks Read %s\n", err.Error())
			return
		}

		err = binary.Read(c, binary.BigEndian, &toHeight)
		if err != nil {
			fmt.Printf("pushBlocks Read %s\n", err.Error())
			return
		}

		var direction int32 = 1
		if toHeight < fromHeight {
			// backwards
			direction = -1
		}

		if toHeight > endHeight {
			toHeight = endHeight
		}

		if fromHeight > endHeight {
			break
		}

		for curHeight := fromHeight; ; curHeight += direction {
			if direction == 1 && curHeight > toHeight {
				// forwards request of height above toHeight
				break
			} else if direction == -1 && curHeight < toHeight {
				// backwards request of height below toHeight
				break
			}
			// over the wire send:
			// 4 byte length prefix for the whole thing
			// then the block, then the udb len, then udb

			// fmt.Printf("push %d\n", curHeight)
			udb, err := util.GetUDataBytesFromFile(curHeight)
			if err != nil {
				fmt.Printf("pushBlocks GetUDataBytesFromFile %s\n", err.Error())
				return
			}
			ud, err := util.UDataFromBytes(udb)
			if err != nil {
				fmt.Printf("pushBlocks UDataFromBytes %s\n", err.Error())
				return
			}

			// set leaf ttl values
			remember := make([]bool, len(ud.UtxoData))
			ud.LeafTTLs = make([]uint32, len(ud.UtxoData))
			for i, utxo := range ud.UtxoData {
				outpointHash := util.HashFromString(utxo.Outpoint.String())
				heightBytes, err := lvdb.Get(outpointHash[:], nil)
				if err != nil {
					if err == leveldb.ErrNotFound {
						// outpoint not spend yet, set leaf ttl to max uint32 value
						ud.LeafTTLs[i] = math.MaxUint32
						remember[i] = ud.LeafTTLs[i]-uint32(curHeight) < 1000
						continue
					}
					panic(err)
				}
				ud.LeafTTLs[i] = binary.BigEndian.Uint32(heightBytes)
				// hardcoded lookahead of 1000 blocks
				remember[i] = ud.LeafTTLs[i]-uint32(curHeight) < 1000
			}
			udb = ud.ToBytes()

			// reduce proof size by removing cached hashes
			needed, cached, _ := clientState.ProbeCache(ud.AccProof.Targets)
			all := mergeSortedSlices(needed, cached)
			var proof []accumulator.Hash
			for i, pos := range all {
				if pos == needed[0] {
					proof = append(proof, ud.AccProof.Proof[i])
					needed = needed[1:]
				}
			}
			fullProof += uint64(len(ud.AccProof.Proof))
			reducedProof += uint64(len(proof))

			if curHeight%100 == 0 {
				fmt.Println("savings: ", fullProof/(reducedProof+1))
			}
			ud.AccProof.Proof = proof

			// fmt.Printf("h %d read %d byte udb\n", curHeight, len(udb))
			blkbytes, err := GetBlockBytesFromFile(curHeight, util.OffsetFilePath, blockDir)
			if err != nil {
				fmt.Printf("pushBlocks GetRawBlockFromFile %s\n", err.Error())
				return
			}

			var b wire.MsgBlock
			b.Deserialize(bytes.NewBuffer(blkbytes))
			_, outskip := util.DedupeBlock(&b)
			clientState.Modify(util.BlockToAddLeaves(b, remember, outskip, curHeight), ud.AccProof.Targets)

			// first send 4 byte length for everything
			// fmt.Printf("h %d send len %d\n", curHeight, len(udb)+len(blkbytes))
			err = binary.Write(c, binary.BigEndian, uint32(len(udb)+len(blkbytes)))
			if err != nil {
				fmt.Printf("pushBlocks binary.Write %s\n", err.Error())
				return
			}
			// next, send the block bytes
			_, err = c.Write(blkbytes)
			if err != nil {
				fmt.Printf("pushBlocks blkbytes write %s\n", err.Error())
				return
			}
			// send 4 byte udata length
			// err = binary.Write(c, binary.BigEndian, uint32(len(udb)))
			// if err != nil {
			// 	fmt.Printf("pushBlocks binary.Write %s\n", err.Error())
			// 	return
			// }
			// last, send the udata bytes
			_, err = c.Write(udb)
			if err != nil {
				fmt.Printf("pushBlocks ubb write %s\n", err.Error())
				return
			}
			// fmt.Printf("wrote %d bytes udb\n", n)
		}

	}
	fmt.Printf("hung up on %s\n", c.RemoteAddr().String())
}

func mergeSortedSlices(a []uint64, b []uint64) (c []uint64) {
	maxa := len(a)
	maxb := len(b)

	// shortcuts:
	if maxa == 0 {
		return b
	}
	if maxb == 0 {
		return a
	}

	// make it (potentially) too long and truncate later
	c = make([]uint64, maxa+maxb)

	idxa, idxb := 0, 0
	for j := 0; j < len(c); j++ {
		// if we're out of a or b, just use the remainder of the other one
		if idxa >= maxa {
			// a is done, copy remainder of b
			j += copy(c[j:], b[idxb:])
			c = c[:j] // truncate empty section of c
			break
		}
		if idxb >= maxb {
			// b is done, copy remainder of a
			j += copy(c[j:], a[idxa:])
			c = c[:j] // truncate empty section of c
			break
		}

		vala, valb := a[idxa], b[idxb]
		if vala < valb { // a is less so append that
			c[j] = vala
			idxa++
		} else if vala > valb { // b is less so append that
			c[j] = valb
			idxb++
		} else { // they're equal
			c[j] = vala
			idxa++
			idxb++
		}
	}
	return
}
