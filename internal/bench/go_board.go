// Package bench contains the three parallel workloads used to measure the
// work-stealing runtime: Go-board playout search, parallel merge sort and
// matrix multiplication. Every workload has a deterministic serial
// reference implementation; parallel runs must produce byte-identical
// results regardless of worker count or steal policy.
package bench

import (
	"math/rand"
)

// Board size for the Go workload.
const BoardSize = 9

// Stone colors.
const (
	Empty = 0
	Black = 1
	White = 2
)

// GoBoard is a fixed 9x9 Go position. Coordinates are row-major index
// r*BoardSize+c. Moves are validated with capture, suicide and simple
// positional superko rules.
type GoBoard struct {
	stones  [BoardSize * BoardSize]uint8
	koPoint int // -1 or the point forbidden by the last capture
	history map[uint64]bool
	zobrist [BoardSize * BoardSize][2]uint64
	hash    uint64
}

// NewGoBoard returns an empty board with deterministic Zobrist keys derived
// from seed so every worker's copy hashes positions identically.
func NewGoBoard(seed int64) *GoBoard {
	b := &GoBoard{koPoint: -1, history: make(map[uint64]bool)}
	rng := rand.New(rand.NewSource(seed))
	for i := range b.zobrist {
		b.zobrist[i][0] = rng.Uint64()
		b.zobrist[i][1] = rng.Uint64()
	}
	return b
}

func opponent(color uint8) uint8 {
	if color == Black {
		return White
	}
	return Black
}

func inBounds(r, c int) bool { return r >= 0 && r < BoardSize && c >= 0 && c < BoardSize }

var neighbors = func() [][]int {
	out := make([][]int, BoardSize*BoardSize)
	for r := 0; r < BoardSize; r++ {
		for c := 0; c < BoardSize; c++ {
			idx := r*BoardSize + c
			for _, d := range [4][2]int{{-1, 0}, {1, 0}, {0, -1}, {0, 1}} {
				nr, nc := r+d[0], c+d[1]
				if inBounds(nr, nc) {
					out[idx] = append(out[idx], nr*BoardSize+nc)
				}
			}
		}
	}
	return out
}()

// groupInfo returns the connected group containing idx, its liberty count
// and the member points.
func (b *GoBoard) groupInfo(idx int) (stones []int, liberties int) {
	color := b.stones[idx]
	seen := make([]bool, BoardSize*BoardSize)
	stack := []int{idx}
	seen[idx] = true
	libSet := make([]bool, BoardSize*BoardSize)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		stones = append(stones, cur)
		for _, nb := range neighbors[cur] {
			switch b.stones[nb] {
			case Empty:
				libSet[nb] = true
			case color:
				if !seen[nb] {
					seen[nb] = true
					stack = append(stack, nb)
				}
			}
		}
	}
	for _, l := range libSet {
		if l {
			liberties++
		}
	}
	return stones, liberties
}

// clone returns an independent copy (including the position history).
func (b *GoBoard) clone() *GoBoard {
	cp := *b
	cp.stones = b.stones
	cp.history = make(map[uint64]bool, len(b.history)+1)
	for h := range b.history {
		cp.history[h] = true
	}
	return &cp
}

// LegalMove reports whether color may play idx.
func (b *GoBoard) LegalMove(idx int, color uint8) bool {
	if idx < 0 || b.stones[idx] != Empty {
		return false
	}
	if idx == b.koPoint {
		return false
	}
	// Tentative placement.
	b.stones[idx] = color
	b.hash ^= b.zobrist[idx][color-1]

	captured := 0
	opp := opponent(color)
	for _, nb := range neighbors[idx] {
		if b.stones[nb] == opp {
			group, libs := b.groupInfo(nb)
			if libs == 0 {
				captured += len(group)
			}
		}
	}
	_, ownLibs := b.groupInfo(idx)
	legal := ownLibs > 0 && !b.history[b.hash]

	// Undo.
	b.hash ^= b.zobrist[idx][color-1]
	b.stones[idx] = Empty
	_ = captured
	return legal
}

// Play applies color's move at idx (assumed legal) and returns the number of
// captured stones.
func (b *GoBoard) Play(idx int, color uint8) int {
	b.stones[idx] = color
	b.hash ^= b.zobrist[idx][color-1]
	b.koPoint = -1

	opp := opponent(color)
	captured := 0
	var singleCapture []int
	for _, nb := range neighbors[idx] {
		if b.stones[nb] == opp {
			group, libs := b.groupInfo(nb)
			if libs == 0 {
				for _, g := range group {
					b.stones[g] = Empty
					b.hash ^= b.zobrist[g][opp-1]
					captured++
				}
				if len(group) == 1 {
					singleCapture = group
				}
			}
		}
	}
	if captured == 1 && len(singleCapture) == 1 {
		// Simple ko: the single captured point is immediately forbidden.
		if group, libs := b.groupInfo(idx); libs == 1 && len(group) == 1 {
			b.koPoint = singleCapture[0]
		}
	}
	b.history[b.hash] = true
	return captured
}

// LegalPoints returns all legal moves for color.
func (b *GoBoard) LegalPoints(color uint8) []int {
	out := make([]int, 0, BoardSize*BoardSize)
	for i := range b.stones {
		if b.LegalMove(i, color) {
			out = append(out, i)
		}
	}
	return out
}

// Score uses area scoring: stones plus surrounded empty points, ties broken
// toward Black. It is deterministic and only needs the final position.
func (b *GoBoard) Score() (black, white int) {
	visited := make([]bool, BoardSize*BoardSize)
	for _, s := range b.stones {
		if s == Black {
			black++
		} else if s == White {
			white++
		}
	}
	for i := range b.stones {
		if b.stones[i] != Empty || visited[i] {
			continue
		}
		region, borders := b.emptyRegion(i, visited)
		if len(borders) == 1 {
			if borders[0] == Black {
				black += len(region)
			} else {
				white += len(region)
			}
		}
	}
	return black, white
}

func (b *GoBoard) emptyRegion(start int, visited []bool) (points []int, borders []uint8) {
	stack := []int{start}
	visited[start] = true
	borderSet := make(map[uint8]bool)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		points = append(points, cur)
		for _, nb := range neighbors[cur] {
			if b.stones[nb] == Empty {
				if !visited[nb] {
					visited[nb] = true
					stack = append(stack, nb)
				}
			} else {
				borderSet[b.stones[nb]] = true
			}
		}
	}
	for color := range borderSet {
		borders = append(borders, color)
	}
	return points, borders
}
