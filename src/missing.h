
/* ISC License                                                              */
/*                                                                          */
/* Copyright (c) 2016, Richard Maw                                          */
/*                                                                          */
/* Permission to use, copy, modify, and/or distribute this software for any */
/* purpose with or without fee is hereby granted, provided that the above   */
/* copyright notice and this permission notice appear in all copies.        */
/*                                                                          */
/* THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES */
/* WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF         */
/* MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR  */
/* ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES   */
/* WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN    */
/* ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF  */
/* OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.           */

#include <unistd.h>      /* syscall */

#if !HAVE_DECL_RENAMEAT2
#include <sys/syscall.h> /* __NR_* */
static inline int renameat2(int oldfd, const char *oldname, int newfd, const char *newname, unsigned flags) {
    return syscall(__NR_renameat2, oldfd, oldname, newfd, newname, flags);
}
#endif

#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1<<0)
#endif
#ifndef RENAME_EXCHANGE
#define RENAME_EXCHANGE (1<<1)
#endif

#if !HAVE_DECL_COPY_FILE_RANGE

#ifndef __NR_copy_file_range
#  if defined(__x86_64__)
#    define __NR_copy_file_range 326
#  elif defined(__i386__)
#    define __NR_copy_file_range 377
#  endif
#endif

static inline int copy_file_range(int fd_in, loff_t *off_in, int fd_out, loff_t *off_out, size_t len, unsigned int flags) {
    return syscall(__NR_copy_file_range, fd_in, off_in, fd_out, off_out, len, flags);
}
#endif
