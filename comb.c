#include "lagent.h"

void swap(char *b, int i, int j)
{
    b[i] ^= b[j];
    b[j] ^= b[i];
    b[i] ^= b[j];
}

void find(char *b, int *i, int n)
{
    int j;
    for (j = 1; j < n; j++) {
        if (!b[j] && b[j - 1]) {
            *i = j - 1;
            return;
        }
    }
    *i = -1;
}

void emit(void *a, char *b, int n)
{
    int *c = (int *)a;
    int i;
    for (i = 0; i < n; i++) {
        if (b[i])
            printf("%d ", c[i]);
    }
    printf("\n");
}

void emit_debug(void *a, char *b, int n)
{
#if COMB_DEBUG
    int i;
    for (i = 0; i < n; i++) {
        printf("EMIT DBG: b[%d] %d\n", i, b[i]);
    }
#endif
}

void left(char *b, int i)
{
    int j, k;
    for (j = 0, k = 0; j < i; j++) {
        if (b[j]) {
            b[k] = b[j];
            if (k != j)
                b[j] = 0;
            k++;
        }
    }
}

void comb(void *a, int n, int m, int index)
{
    char *b = malloc(n);
    int i, j = 0;
    if (!b || !m)
        return;

    memset(b, 0, n);
    for (i = 0; i < m; i++)
        b[i] = 1;

    do {
        find(b, &i, n);
        emit_debug(a, b, n);
        if (j == index || index == -1)
            comb_emit_cb(a, b, n);
        if (i < 0 || j == index)
            break;
        swap(b, i, i + 1);
        left(b, i);
        j++;
    } while (i != -1);
    free(b);
}

#if MAIN
ce_cb_t comb_emit_cb = &emit;

int main()
{
    int a[10] = {0,1,2,3,4,5,6,7,8,9};

    comb(a, 10, 5, -1);
    return 0;
}
#endif
