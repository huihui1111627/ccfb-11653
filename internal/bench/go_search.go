package bench

import (
	"fmt"
	"math/rand"

	"gosteal/internal/ws"
)

// GoResult is the analysis of one legal root move.
type GoResult struct {
	Move       int     `json:"move"`
	Playouts   int     `json:"playouts"`
	BlackWins  int     `json:"blackWins"`
	WinRate    float64 `json:"winRate"` // Black's win rate from this move
	BlackScore float64 `json:"blackScore"`
	WhiteScore float64 `json:"whiteScore"`
}

// GoConfig sizes the Go workload.
type GoConfig struct {
	Seed      int64 // base seed for board and playout RNGs
	Playouts  int   // random playouts per root move
	MaxMoves  int   // move cap inside one playout
	Grain     int   // ParallelFor leaf grain (root moves per task)
	RootColor uint8 // color whose root moves are analyzed
}

// DefaultGoConfig is sized to give a serial runtime in the low seconds.
func DefaultGoConfig() GoConfig {
	return GoConfig{
		Seed:      20240521,
		Playouts:  24,
		MaxMoves:  BoardSize * BoardSize * 2,
		Grain:     1,
		RootColor: Black,
	}
}

// playout runs one deterministic random game from the given board. The RNG
// is seeded from (baseSeed, moveIndex, playoutIndex) so results depend only
// on the inputs, never on scheduling or goroutine identity.
func playout(start *GoBoard, rootMove int, rootColor uint8, rng *rand.Rand, maxMoves int) (black, white int, winner uint8) {
	b := start.clone()
	color := rootColor
	b.Play(rootMove, color)
	color = opponent(color)

	passes := 0
	for move := 1; move < maxMoves && passes < 2; move++ {
		legal := b.LegalPoints(color)
		if len(legal) == 0 || rng.Intn(64) == 0 {
			// ~1/64 chance of passing when moves exist, plus mandatory pass.
			passes++
			color = opponent(color)
			continue
		}
		passes = 0
		choice := legal[rng.Intn(len(legal))]
		b.Play(choice, color)
		color = opponent(color)
	}
	black, white = b.Score()
	if black >= white {
		winner = Black
	} else {
		winner = White
	}
	return black, white, winner
}

// analyzeMove runs all playouts for one root move and aggregates them.
func analyzeMove(board *GoBoard, move int, cfg GoConfig) GoResult {
	res := GoResult{Move: move, Playouts: cfg.Playouts}
	var blackScoreSum, whiteScoreSum int64
	for k := 0; k < cfg.Playouts; k++ {
		seed := cfg.Seed ^ int64(move)*1_000_003 ^ int64(k)*97
		rng := rand.New(rand.NewSource(seed))
		bs, ws, winner := playout(board, move, cfg.RootColor, rng, cfg.MaxMoves)
		blackScoreSum += int64(bs)
		whiteScoreSum += int64(ws)
		if winner == Black {
			res.BlackWins++
		}
	}
	res.WinRate = float64(res.BlackWins) / float64(cfg.Playouts)
	res.BlackScore = float64(blackScoreSum) / float64(cfg.Playouts)
	res.WhiteScore = float64(whiteScoreSum) / float64(cfg.Playouts)
	return res
}

// RunGoSerial analyzes every legal root move on one goroutine.
func RunGoSerial(cfg GoConfig) []GoResult {
	board := NewGoBoard(cfg.Seed)
	moves := board.LegalPoints(cfg.RootColor)
	out := make([]GoResult, len(moves))
	for i, m := range moves {
		out[i] = analyzeMove(board, m, cfg)
	}
	return out
}

// RunGoParallel analyzes root moves with the work-stealing pool, splitting
// the move range by recursive bisection at the configured grain.
func RunGoParallel(pool *ws.Pool, cfg GoConfig) ([]GoResult, error) {
	board := NewGoBoard(cfg.Seed)
	moves := board.LegalPoints(cfg.RootColor)
	out := make([]GoResult, len(moves))
	err := pool.Run(fmt.Sprintf("go-search(%d moves)", len(moves)), func(ctx *ws.TaskCtx) error {
		return ctx.ParallelFor(0, len(moves), cfg.Grain, "go-root", func(ctx *ws.TaskCtx, lo, hi int) error {
			for i := lo; i < hi; i++ {
				out[i] = analyzeMove(board, moves[i], cfg)
			}
			return nil
		})
	})
	return out, err
}
