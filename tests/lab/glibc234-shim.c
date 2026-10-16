/* Lab-only linker wrappers for C dependencies built on a newer glibc.
 * Do not use in production: these restore pre-C23 integer parsing semantics.
 */
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

int __wrap___isoc23_sscanf(const char *input, const char *format, ...) {
    va_list args;
    va_start(args, format);
    int result = vsscanf(input, format, args);
    va_end(args);
    return result;
}

long __wrap___isoc23_strtol(const char *input, char **end, int base) {
    return strtol(input, end, base);
}
