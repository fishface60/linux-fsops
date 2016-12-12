
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

#include <assert.h>      /* assert */
#include <errno.h>       /* errno, ENOSYS */
#include <getopt.h>      /* struct option, getopt_long */
#include <sys/types.h>   /* mode_t */
#include <fcntl.h>       /* AT_*, O_*, open */
#include <unistd.h>      /* close, unlink */
#include <stdio.h>       /* rename* */
#include <limits.h>      /* SSIZE_MAX */

#include "clobber.h"     /* CLOBBER_* */
#include "copy.h"        /* copy_contents */
#include "missing.h"     /* RENAME_*, SEEK_*, renameat2 */

static int create_file(const char *path, mode_t mode, int flags,
                       enum clobber clobber) {
    switch (clobber) {
        case CLOBBER_PERMITTED:
            flags |= O_CREAT;
            break;
        case CLOBBER_REQUIRED:
        case CLOBBER_TRY_REQUIRED:
            flags &= ~O_CREAT;
            break;
        case CLOBBER_FORBIDDEN:
        case CLOBBER_TRY_FORBIDDEN:
            flags |= O_CREAT|O_EXCL;
            break;
        default:
            assert(0);
    }
    return open(path, flags, mode);
}

static int rename_file(const char *src, const char *tgt, enum clobber clobber) {
    int ret = -1;
    int renameflags = 0;

    switch (clobber) {
        case CLOBBER_REQUIRED:
        case CLOBBER_TRY_REQUIRED:
            renameflags = RENAME_EXCHANGE;
            break;
        case CLOBBER_FORBIDDEN:
        case CLOBBER_TRY_FORBIDDEN:
            renameflags = RENAME_NOREPLACE;
            break;
        default:
            assert(0);
    }

    ret = renameat2(AT_FDCWD, src, AT_FDCWD, tgt, renameflags);
    if (ret == 0) {
        if (clobber == CLOBBER_REQUIRED || clobber == CLOBBER_TRY_REQUIRED) {
            ret = unlink(src);
        }
        return ret;
    }

    if ((errno == ENOSYS || errno == EINVAL)
        && (clobber != CLOBBER_REQUIRED
            && clobber != CLOBBER_FORBIDDEN)) {
        ret = rename(src, tgt);
    }

cleanup:
    return ret;
}

int main(int argc, char *argv[]) {
    static const struct option opts[] = {
        { .name = "clobber-permitted",     .has_arg = no_argument,
          .val = CLOBBER_PERMITTED, },
        { .name = "clobber-required",      .has_arg = no_argument,
          .val = CLOBBER_REQUIRED, },
        { .name = "clobber-try-required",  .has_arg = no_argument,
          .val = CLOBBER_TRY_REQUIRED, },
        { .name = "clobber-forbidden",     .has_arg = no_argument,
          .val = CLOBBER_FORBIDDEN, },
        { .name = "clobber-try-forbidden", .has_arg = no_argument,
          .val = CLOBBER_TRY_FORBIDDEN, },
        {},
    };

    enum clobber clobber = CLOBBER_PERMITTED;
    for (;;) {
        int ret = getopt_long(argc, argv, "prRnN", opts, NULL);
        if (ret == -1)
            break;

        switch(ret) {
            case CLOBBER_PERMITTED:
            case CLOBBER_REQUIRED:
            case CLOBBER_TRY_REQUIRED:
            case CLOBBER_FORBIDDEN:
            case CLOBBER_TRY_FORBIDDEN:
                clobber = ret;
                break;
            case '?':
            default:
                return 1;
        }
    }

    if (optind == argc || argc > optind + 2) {
        return 1;
    }

    if (argc == optind + 1) {
        int ret = 0;
        int fd = create_file(argv[optind], 0666, O_WRONLY, clobber);
        if (fd < 0)
            return 1;

        do {
            ret = copy_contents(0, fd);
        } while (ret > 0);

        if (ret != 0)
            return 1;

        close(fd);
    } else {
        return rename_file(argv[optind], argv[optind + 1], clobber);
    }
    return 0;
}
