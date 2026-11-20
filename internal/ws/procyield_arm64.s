//go:build arm64

#include "textflag.h"

// func procyield(n int)
TEXT ·procyield(SB),NOSPLIT,$0-8
	MOVW n+0(FP), R0
loop:
	CBZ  R0, done
	YIELD
	SUBW $1, R0
	B    loop
done:
	RET
