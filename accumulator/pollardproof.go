package accumulator

import (
	"fmt"
)

type miniTree struct {
	parent, left, right node
}

// IngestBatchProof populates the Pollard with all needed data to delete the
// targets in the block proof
func (p *Pollard) IngestBatchProof(bp BatchProof) error {
	// verify the batch proof.
	rootHashes := p.rootHashesReverse()
	ok, trees, roots := verifyBatchProof(bp, rootHashes, p.numLeaves,
		// pass a closure that checks the pollard for cached nodes.
		// returns true and the hash value of the node if it exists.
		// returns false if the node does not exist or the hash value is empty.
		func(pos uint64) (bool, Hash) {
			n, _, _, err := p.readPos(pos)
			if err != nil {
				return false, empty
			}
			if n != nil && n.data != empty {
				return true, n.data
			}

			return false, empty
		})
	if !ok {
		return fmt.Errorf("block proof mismatch")
	}
	// preallocating polNodes helps with garbage collection
	polNodes := make([]polNode, len(trees)*3)
	i := 0
	nodesAllocated := 0
	for _, root := range roots {
		for root.Val != rootHashes[i] {
			i++
		}
		// populate the pollard
		nodesAllocated += p.populate(p.roots[len(p.roots)-i-1], root.Pos,
			trees, polNodes[nodesAllocated:])
	}

	return nil
}

// populate takes a root and populates it with the nodes of the paritial proof tree that was computed
// in `verifyBatchProof`.
func (p *Pollard) populate(root *polNode, pos uint64, trees []miniTree, polNodes []polNode) int {
	if len(trees) == 0 {
		return 0
	}

	// a stack to traverse the pollard
	type stackElem struct {
		pos   uint64
		trees []miniTree
		node  *polNode
	}
	stack := make([]stackElem, len(trees)*2)
	stackIndex := 0
	// Put first elem on the stack (the root)
	stack[stackIndex] = stackElem{pos, trees, root}

	rows := p.rows()
	nodesAllocated := 0
	for stackIndex >= 0 {
		// Pop elem from stack
		elem := stack[stackIndex]

		if elem.pos < p.numLeaves {
			// this is a leaf, we are done populating this branch.
			stackIndex--
			continue
		}

		leftChild := child(elem.pos, rows)
		rightChild := child(elem.pos, rows) | 1
		var left, right *polNode
		i := 0
	find_nodes:
		for ; i < len(elem.trees); i++ {
			switch {
			case elem.trees[i].parent.Pos == elem.pos:
				fallthrough
			case elem.trees[i].parent.Pos == rightChild:
				if elem.node.niece[0] == nil {
					elem.node.niece[0] = &polNodes[nodesAllocated]
					nodesAllocated++
				}
				right = elem.node.niece[0]
				right.data = elem.trees[i].left.Val
				fallthrough
			case elem.trees[i].parent.Pos == leftChild:
				if elem.node.niece[1] == nil {
					elem.node.niece[1] = &polNodes[nodesAllocated]
					nodesAllocated++
				}
				left = elem.node.niece[1]
				left.data = elem.trees[i].right.Val
				break find_nodes
			}
		}
		if i >= len(elem.trees) {
			stackIndex--
			continue
		}

		// Put the next two elems on stack (right/left branch)
		stack[stackIndex] = stackElem{leftChild, trees[i+1:], left} // technically the pop happens here because we override `elem`

		stackIndex++
		if stackIndex >= len(stack) {
			// TODO: we could resize here, if this actually happens
			panic("populate() stack was too small")
		}

		stack[stackIndex] = stackElem{rightChild, trees[i+1:], right}
		// No stackIndex-- here because we pop this last added elem next iteration
	}
	return nodesAllocated
}
