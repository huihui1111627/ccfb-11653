//go:build amd64

#include "textflag.h"

// func procyield(n int)
TEXT ·procyield(SB),NOSPLIT,$0-8
	MOVL n+0(FP), AX
loop:
	TESTL AX, AX
	JZ    done
	PAUSE
	DECL  AX
	JMP   loop
done:
	RET
