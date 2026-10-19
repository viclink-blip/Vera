# Vera Bot - strategy maths in x86-64 assembly (System V ABI, Linux).
# Build:  ./build_asm.sh   ->  libvera_asm.so   (loaded by vera_asm.py)
#
#   double vera_sma(const double *x, long n, long period)
#       mean of the last `period` values of x[0..n-1]; NaN if n < period
#   double vera_rsi(const double *close, long n, long period)
#       Wilder's RSI of the closes (same as pandas ewm(alpha=1/period, adjust=False));
#       100 when there were no losses, NaN if n < 2
#   void vera_digit_hist(const long *digits, long n, long *out10)
#       out10[d] = how many times digit d (0-9) appears

        .intel_syntax noprefix
        .text

# ---------------------------------------------------------------- vera_sma
        .globl  vera_sma
        .type   vera_sma, @function
vera_sma:                               # rdi = x, rsi = n, rdx = period
        test    rdx, rdx
        jle     .Lsma_nan
        cmp     rsi, rdx
        jl      .Lsma_nan
        mov     rcx, rsi
        sub     rcx, rdx                # index of the first value in the window
        lea     rax, [rdi + rcx*8]
        pxor    xmm0, xmm0              # sum = 0
        mov     rcx, rdx
.Lsma_loop:
        addsd   xmm0, qword ptr [rax]
        add     rax, 8
        dec     rcx
        jnz     .Lsma_loop
        cvtsi2sd xmm1, rdx
        divsd   xmm0, xmm1              # sum / period
        ret
.Lsma_nan:
        movsd   xmm0, qword ptr [rip + .Lnan]
        ret
        .size   vera_sma, .-vera_sma

# ---------------------------------------------------------------- vera_rsi
        .globl  vera_rsi
        .type   vera_rsi, @function
vera_rsi:                               # rdi = close, rsi = n, rdx = period
        cmp     rsi, 2
        jl      .Lrsi_nan
        test    rdx, rdx
        jle     .Lrsi_nan
        cvtsi2sd xmm7, rdx
        movsd   xmm6, qword ptr [rip + .Lone]
        divsd   xmm6, xmm7              # a = 1 / period
        movsd   xmm5, qword ptr [rip + .Lone]
        subsd   xmm5, xmm6              # 1 - a
        pxor    xmm2, xmm2              # G = average gain
        pxor    xmm3, xmm3              # L = average loss
        mov     rcx, 1
.Lrsi_loop:
        movsd   xmm0, qword ptr [rdi + rcx*8]
        subsd   xmm0, qword ptr [rdi + rcx*8 - 8]   # d = close[i] - close[i-1]
        pxor    xmm1, xmm1
        maxsd   xmm1, xmm0              # gain = max(0, d)
        pxor    xmm4, xmm4
        subsd   xmm4, xmm0              # -d
        pxor    xmm8, xmm8
        maxsd   xmm8, xmm4              # loss = max(0, -d)
        cmp     rcx, 1
        jne     .Lrsi_update
        movapd  xmm2, xmm1              # first move starts the averages
        movapd  xmm3, xmm8
        jmp     .Lrsi_next
.Lrsi_update:
        mulsd   xmm2, xmm5              # G = (1-a)*G + a*gain
        mulsd   xmm1, xmm6
        addsd   xmm2, xmm1
        mulsd   xmm3, xmm5              # L = (1-a)*L + a*loss
        mulsd   xmm8, xmm6
        addsd   xmm3, xmm8
.Lrsi_next:
        inc     rcx
        cmp     rcx, rsi
        jl      .Lrsi_loop

        pxor    xmm0, xmm0
        ucomisd xmm3, xmm0
        je      .Lrsi_100               # no losses at all -> RSI 100
        divsd   xmm2, xmm3              # RS = G / L
        addsd   xmm2, qword ptr [rip + .Lone]
        movsd   xmm1, qword ptr [rip + .Lhundred]
        divsd   xmm1, xmm2              # 100 / (1 + RS)
        movsd   xmm0, qword ptr [rip + .Lhundred]
        subsd   xmm0, xmm1              # 100 - 100 / (1 + RS)
        ret
.Lrsi_100:
        movsd   xmm0, qword ptr [rip + .Lhundred]
        ret
.Lrsi_nan:
        movsd   xmm0, qword ptr [rip + .Lnan]
        ret
        .size   vera_rsi, .-vera_rsi

# ---------------------------------------------------------------- vera_digit_hist
        .globl  vera_digit_hist
        .type   vera_digit_hist, @function
vera_digit_hist:                        # rdi = digits, rsi = n, rdx = out10
        xor     ecx, ecx
.Lhist_zero:
        mov     qword ptr [rdx + rcx*8], 0
        inc     rcx
        cmp     rcx, 10
        jl      .Lhist_zero
        test    rsi, rsi
        jle     .Lhist_done
        xor     ecx, ecx
.Lhist_loop:
        mov     rax, qword ptr [rdi + rcx*8]
        cmp     rax, 9
        ja      .Lhist_skip             # unsigned compare also skips negatives
        inc     qword ptr [rdx + rax*8]
.Lhist_skip:
        inc     rcx
        cmp     rcx, rsi
        jl      .Lhist_loop
.Lhist_done:
        ret
        .size   vera_digit_hist, .-vera_digit_hist

        .section .rodata
        .align  8
.Lone:      .double 1.0
.Lhundred:  .double 100.0
.Lnan:      .quad   0x7ff8000000000000

        .section .note.GNU-stack, "", @progbits
