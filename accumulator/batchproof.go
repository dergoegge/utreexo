package accumulator

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// BatchProof :
type BatchProof struct {
	Targets []uint64
	Proof   []Hash
	// list of leaf locations to delete, along with a bunch of hashes that give the proof.
	// the position of the hashes is implied / computable from the leaf positions
}

// ToBytes give the bytes for a BatchProof.  It errors out silently because
// I don't think the binary.Write errors ever actually happen
func (bp *BatchProof) ToBytes() []byte {
	var buf bytes.Buffer

	// first write the number of targets (4 byte uint32)
	numTargets := uint32(len(bp.Targets))
	if numTargets == 0 {
		return nil
	}
	err := binary.Write(&buf, binary.BigEndian, numTargets)
	if err != nil {
		fmt.Printf("huh %s\n", err.Error())
		return nil
	}
	for _, t := range bp.Targets {
		// there's no need for these to be 64 bit for the next few decades...
		err := binary.Write(&buf, binary.BigEndian, t)
		if err != nil {
			fmt.Printf("huh %s\n", err.Error())
			return nil
		}
	}
	// then the rest is just hashes
	for _, h := range bp.Proof {
		_, err = buf.Write(h[:])
		if err != nil {
			fmt.Printf("huh %s\n", err.Error())
			return nil
		}
	}

	return buf.Bytes()
}

// ToString for debugging, shows the blockproof
func (bp *BatchProof) SortTargets() {
	sortUint64s(bp.Targets)
}

// ToString for debugging, shows the blockproof
func (bp *BatchProof) ToString() string {
	s := fmt.Sprintf("%d targets: ", len(bp.Targets))
	for _, t := range bp.Targets {
		s += fmt.Sprintf("%d\t", t)
	}
	s += fmt.Sprintf("\n%d proofs: ", len(bp.Proof))
	for _, p := range bp.Proof {
		s += fmt.Sprintf("%04x\t", p[:4])
	}
	s += "\n"
	return s
}

// FromBytesBatchProof gives a block proof back from the serialized bytes
func FromBytesBatchProof(b []byte) (BatchProof, error) {
	var bp BatchProof

	// if empty slice, return empty BatchProof with 0 targets
	if len(b) == 0 {
		return bp, nil
	}
	// otherwise, if there are less than 4 bytes we can't even see the number
	// of targets so something is wrong
	if len(b) < 4 {
		return bp, fmt.Errorf("batchproof only %d bytes", len(b))
	}

	buf := bytes.NewBuffer(b)
	// read 4 byte number of targets
	var numTargets uint32
	err := binary.Read(buf, binary.BigEndian, &numTargets)
	if err != nil {
		return bp, err
	}
	bp.Targets = make([]uint64, numTargets)
	for i, _ := range bp.Targets {
		err := binary.Read(buf, binary.BigEndian, &bp.Targets[i])
		if err != nil {
			return bp, err
		}
	}
	remaining := buf.Len()
	// the rest is hashes
	if remaining%32 != 0 {
		return bp, fmt.Errorf("%d bytes left, should be n*32", buf.Len())
	}
	bp.Proof = make([]Hash, remaining/32)

	for i, _ := range bp.Proof {
		copy(bp.Proof[i][:], buf.Next(32))
	}
	return bp, nil
}

// verifyBatchProof verifies a batchproof by checking against the set of known correct roots.
// Takes a BatchProof, the accumulator roots, and the number of leaves in the forest.
// Returns wether or not the proof verified correctly, the partial proof tree,
// and the subset of roots that was computed.
func verifyBatchProof(bp BatchProof, roots []Hash, numLeaves uint64,
	// cached should be a function that fetches nodes from the pollard and indicates whether they
	// exist or not, can be nil for the forest
	cached func(pos uint64) (bool, Hash)) (bool, [][3]node, []node) {
	if len(bp.Targets) == 0 {
		return true, nil, nil
	}

	if cached == nil {
		cached = func(_ uint64) (bool, Hash) { return false, empty }
	}

	rows := treeRows(numLeaves)
	proofs := bp.Proof
	proofPositions, computablePositions := ProofPositions(bp.Targets, numLeaves, rows)
	// targetNodes holds nodes that are known, on the bottom row those are the targets,
	// on the upper rows it holds computed nodes.
	// rootCandidates holds the roots that where computed, and have to be compared to the actual roots
	// at the end.
	targetNodes := make([]node, 0, len(bp.Targets)*int(rows))
	rootCandidates := make([]node, 0, len(roots))
	// trees is a slice of 3-Tuples, each tuple represents a parent and its children.
	// tuple[0] is the parent, tuple[1] is the left child and tuple[2] is the right child.
	// trees holds the entire proof tree of the batchproof in this way, sorted by the tuple[0].
	trees := make([][3]node, 0, len(computablePositions))
	// initialise the targetNodes for row 0.
	// TODO: this would be more straight forward if bp.Proofs wouldn't contain the targets
	proofHashes := make([]Hash, 0, len(proofPositions))
	targets := bp.Targets
	var targetsMatched uint64
	for len(targets) > 0 {
		if targets[0] == numLeaves-1 && numLeaves&1 == 1 {
			// row 0 root.
			rootCandidates = append(rootCandidates, node{Val: roots[0], Pos: targets[0]})
			proofs = proofs[1:]
			break
		}

		if uint64(len(proofPositions)) > targetsMatched &&
			targets[0]^1 == proofPositions[targetsMatched] {
			lr := targets[0] & 1
			targetNodes = append(targetNodes, node{Pos: targets[0], Val: proofs[lr]})
			proofHashes = append(proofHashes, proofs[lr^1])
			targetsMatched++
			proofs = proofs[2:]
			targets = targets[1:]
			continue
		}

		if len(proofs) < 2 || len(targets) < 2 {
			return false, nil, nil
		}

		targetNodes = append(targetNodes,
			node{Pos: targets[0], Val: proofs[0]},
			node{Pos: targets[1], Val: proofs[1]})
		proofs = proofs[2:]
		targets = targets[2:]
	}

	proofHashes = append(proofHashes, proofs...)
	proofs = proofHashes

	// hash every target node with its sibling (which either is contained in the proof or also a target)
	for len(targetNodes) > 0 {
		var target, proof node
		target = targetNodes[0]
		if len(proofPositions) > 0 && target.Pos^1 == proofPositions[0] {
			// target has a sibling in the proof positions, fetch proof
			proof = node{Pos: proofPositions[0], Val: proofs[0]}
			proofPositions = proofPositions[1:]
			proofs = proofs[1:]
			targetNodes = targetNodes[1:]
			goto hash
		}

		// target should have its sibling in targetNodes
		if len(targetNodes) == 1 {
			// sibling not found
			return false, nil, nil
		}

		proof = targetNodes[1]
		targetNodes = targetNodes[2:]

	hash:
		// figure out which node is left and which is right
		left := target
		right := proof
		if target.Pos&1 == 1 {
			right, left = left, right
		}

		// get the hash of the parent from the cache or compute it
		parentPos := parent(target.Pos, rows)
		isParentCached, hash := cached(parentPos)
		if !isParentCached {
			hash = parentHash(left.Val, right.Val)
		}
		trees = append(trees, [3]node{{Val: hash, Pos: parentPos}, left, right})

		row := detectRow(parentPos, rows)
		if numLeaves&(1<<row) > 0 && parentPos == rootPosition(numLeaves, row, rows) {
			// the parent is a root -> store as candidate, to check against actual roots later.
			rootCandidates = append(rootCandidates, node{Val: hash, Pos: parentPos})
			continue
		}
		targetNodes = append(targetNodes, node{Val: hash, Pos: parentPos})
	}
	if len(rootCandidates) == 0 {
		// no roots to verify
		return false, nil, nil
	}
	rootMatches := 0
	for _, root := range roots {
		if len(rootCandidates) > rootMatches && root == rootCandidates[rootMatches].Val {
			rootMatches++
		}
	}
	if len(rootCandidates) != rootMatches {
		// some roots did not match
		return false, nil, nil
	}

	return true, trees, rootCandidates
}

// Reconstruct takes a number of leaves and rows, and turns a block proof back
// into a partial proof tree. Should leave bp intact
func (bp *BatchProof) Reconstruct(
	numleaves uint64, forestRows uint8) (map[uint64]Hash, error) {

	if verbose {
		fmt.Printf("reconstruct blockproof %d tgts %d hashes nl %d fr %d\n",
			len(bp.Targets), len(bp.Proof), numleaves, forestRows)
	}
	proofTree := make(map[uint64]Hash)

	// If there is nothing to reconstruct, return empty map
	if len(bp.Targets) == 0 {
		return proofTree, nil
	}
	proof := bp.Proof // back up proof
	targets := bp.Targets
	rootPositions, rootRows := getRootsReverse(numleaves, forestRows)

	if verbose {
		fmt.Printf("%d roots:\t", len(rootPositions))
		for _, t := range rootPositions {
			fmt.Printf("%d ", t)
		}
	}
	// needRow / nextrow hold the positions of the data which should be in the blockproof
	var needSibRow, nextRow []uint64 // only even siblings needed

	// a bit strange; pop off 2 hashes at a time, and either 1 or 2 positions
	for len(bp.Proof) > 0 && len(targets) > 0 {

		if targets[0] == rootPositions[0] {
			// target is a root; this can only happen at row 0;
			// there's a "proof" but don't need to actually send it
			if verbose {
				fmt.Printf("placed single proof at %d\n", targets[0])
			}
			proofTree[targets[0]] = bp.Proof[0]
			bp.Proof = bp.Proof[1:]
			targets = targets[1:]
			continue
		}

		// there should be 2 proofs left then
		if len(bp.Proof) < 2 {
			return nil, fmt.Errorf("only 1 proof left but need 2 for %d",
				targets[0])
		}

		// populate first 2 proof hashes
		right := targets[0] | 1
		left := right ^ 1

		proofTree[left] = bp.Proof[0]
		proofTree[right] = bp.Proof[1]
		needSibRow = append(needSibRow, parent(targets[0], forestRows))
		// pop em off
		if verbose {
			fmt.Printf("placed proofs at %d, %d\n", left, right)
		}
		bp.Proof = bp.Proof[2:]

		if len(targets) > 1 && targets[0]|1 == targets[1] {
			// pop off 2 positions
			targets = targets[2:]
		} else {
			// only pop off 1
			targets = targets[1:]
		}
	}

	// deal with 0-row root, regardless of whether it was used or not
	if rootRows[0] == 0 {
		rootPositions = rootPositions[1:]
		rootRows = rootRows[1:]
	}

	// now all that's left is the proofs. go bottom to root and iterate the haveRow
	for h := uint8(1); h < forestRows; h++ {
		for len(needSibRow) > 0 {
			// if this is a root, it's not needed or given
			if needSibRow[0] == rootPositions[0] {
				needSibRow = needSibRow[1:]
				rootPositions = rootPositions[1:]
				rootRows = rootRows[1:]
				continue
			}
			// either way, we'll get the parent
			nextRow = append(nextRow, parent(needSibRow[0], forestRows))

			// if we have both siblings here, don't need any proof
			if len(needSibRow) > 1 && needSibRow[0]^1 == needSibRow[1] {
				needSibRow = needSibRow[2:]
			} else {
				// return error if we need a proof and can't get it
				if len(bp.Proof) == 0 {
					fmt.Printf("roots %v needsibrow %v\n", rootPositions, needSibRow)
					return nil, fmt.Errorf("h %d no proofs left at pos %d ",
						h, needSibRow[0]^1)
				}
				// otherwise we do need proof; place in sibling position and pop off
				proofTree[needSibRow[0]^1] = bp.Proof[0]
				bp.Proof = bp.Proof[1:]
				// and get rid of 1 element of needSibRow
				needSibRow = needSibRow[1:]
			}
		}

		// there could be a root on this row that we don't need / use; if so pop it
		if len(rootRows) > 0 && rootRows[0] == h {
			rootPositions = rootPositions[1:]
			rootRows = rootRows[1:]
		}

		needSibRow = nextRow
		nextRow = []uint64{}
	}
	if len(bp.Proof) != 0 {
		return nil, fmt.Errorf("too many proofs, %d remain", len(bp.Proof))
	}
	bp.Proof = proof // restore from backup
	return proofTree, nil
}
