
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

#include <stdlib.h>          /* NULL */
#include <linux/btrfs.h>     /* BTRFS_IOC_CLONE */
#include <linux/magic.h>     /* BTRFS_SUPER_MAGIC */
#include <fcntl.h>           /* splice */
#include <sys/types.h>       /* off_t, ssize_t */
#include <sys/sendfile.h>    /* sendfile */
#include <errno.h>           /* errno, E* */
#include <stdio.h>           /* perror */
#include <stdbool.h>         /* true, false */
#include <limits.h>          /* SSIZE_MAX */
#include <sys/vfs.h>         /* ftatfs, struct statfs */
#include <sys/stat.h>        /* statfs, struct stat */
#include <sys/ioctl.h>       /* ioctl */

#include "copy.h"
#include "missing.h"         /* renameat2, RENAME_*, SEEK_*, copy_file_range */

static ssize_t cfr_copy_range(int srcfd, int tgtfd, size_t range) {
    size_t to_copy = range;
    while (to_copy) {
        ssize_t ret = copy_file_range(srcfd, NULL, tgtfd, NULL, range, 0);
        if (ret < 0)
            return ret;
        to_copy -= ret;
    }
    return range;
}

static ssize_t sendfile_copy_range(int srcfd, int tgtfd, size_t range) {
    size_t to_copy = range;
    while (to_copy) {
        ssize_t ret = sendfile(tgtfd, srcfd, NULL, range);
        if (ret < 0)
            return ret;
        to_copy -= ret;
    }
    return range;
}

static ssize_t splice_copy_range(int srcfd, int tgtfd, size_t range) {
    size_t to_copy = range;
    while (to_copy) {
        ssize_t ret = splice(srcfd, NULL, tgtfd, NULL, range, 0);
        if (ret < 0)
            return ret;
        to_copy -= ret;
    }
    return range;
}

static ssize_t naive_copy_range(int srcfd, int tgtfd, size_t range) {
    char buf[4 * 1024 * 1024];
    size_t copied = 0;
    while (range > copied) {
        size_t to_copy = range - copied;
        ssize_t n_read;
        n_read = TEMP_FAILURE_RETRY(read(srcfd, buf,
                to_copy > sizeof(buf) ? sizeof(buf) : to_copy));
        if (n_read < 0) {
            perror("Read source file");
            return n_read;
        }
        if (n_read == 0)
            break;

        while (n_read > 0) {
            ssize_t n_written = TEMP_FAILURE_RETRY(write(tgtfd, buf, n_read));
            if (n_written < 0)
                perror("Write to target file");
                return n_written;

            n_read -= n_written;
            copied += n_written;
        }
    }
    return copied;
}

static ssize_t copy_range(int srcfd, int tgtfd, size_t range) {
    ssize_t copied;
    static int have_cfr = true, have_sendfile = true, have_splice = true;

    if (have_cfr) {
        copied = cfr_copy_range(srcfd, tgtfd, range);
        if (copied >= 0) {
            return copied;
        } else if (errno == ENOSYS) {
            have_cfr = false;
        } else if (errno != EINVAL) {
            return copied;
        }
    }

    if (have_sendfile) {
        copied = sendfile_copy_range(srcfd, tgtfd, range);
        if (copied >= 0) {
            return copied;
        } else if (errno == ENOSYS) {
            have_sendfile = false;
        } else if (errno != EINVAL) {
            return copied;
        }
    }

    if (have_splice) {
        copied = splice_copy_range(srcfd, tgtfd, range);
        if (copied >= 0) {
            return copied;
        } else if (errno == ENOSYS) {
            have_splice = false;
        } else if (errno != EINVAL) {
            return copied;
        }
    }

    return naive_copy_range(srcfd, tgtfd, range);
}

static int naive_contents_copy(int srcfd, int tgtfd) {
    ssize_t ret;
    ssize_t copied = 0;
    do {
        ret = copy_range(srcfd, tgtfd, SSIZE_MAX);
        if (ret < 0)
            return ret;
        copied += ret;
    } while (ret != SSIZE_MAX);
    return copied;
} 

static ssize_t sparse_copy_contents(int srcfd, int tgtfd) {
    size_t copied = 0;
    off_t srcoffs = (off_t)-1;
    off_t nextoffs = (off_t)-1;

    srcoffs = TEMP_FAILURE_RETRY(lseek(srcfd, 0, SEEK_CUR));
    if (srcoffs == (off_t)-1) {
        perror("Find current position of file");
        /* Can't seek file, could be file isn't seekable,
           or that the current offset would overflow. */
        return -1;
    }

    nextoffs = TEMP_FAILURE_RETRY(lseek(srcfd, srcoffs, SEEK_DATA));
    if (nextoffs == (off_t)-1) {
        if (errno == ENXIO) {
            /* NXIO means EOF, there is no data to copy,
               but we may need to make a hole to the end of the file */
            goto end_hole;
        }
        perror("Find data or hole at beginning of file");
        /* Error seeking, must not support sparse seek */
        return -1;
    }

    if (srcoffs != nextoffs)
        /* Seeked to the end of a hole, can skip a data copy. */
        goto hole;

    for (;;) {
        ssize_t ret;
        /* In data, so we must find the end of the data then copy it,
           could pread/write. */
        nextoffs = TEMP_FAILURE_RETRY(lseek(srcfd, srcoffs, SEEK_HOLE));
        if (nextoffs == (off_t)-1) {
            if (errno != ENXIO) {
                perror("Find end of data");
                return -1;
            }

            /* EOF after data, but we still need to copy */
            goto end_data;
        }

        srcoffs = TEMP_FAILURE_RETRY(lseek(srcfd, srcoffs, SEEK_SET));
        if (srcoffs == (off_t)-1) {
            /* Rewinding failed, something is *very* strange. */
            perror("Rewind back to data");
            return -1;
        }

        ret = copy_range(srcfd, tgtfd, nextoffs - srcoffs);
        if (ret < 0) {
            return -1;
        }
        copied += ret;
        srcoffs = nextoffs;

        nextoffs = TEMP_FAILURE_RETRY(lseek(srcfd, srcoffs, SEEK_DATA));
        if (nextoffs == (off_t)-1) {
            if (errno == ENXIO) {
                /* NXIO means EOF, there is no data to copy,
                   but we may need to make a hole to the end of the file */
                goto end_hole;
            }
            perror("Find end of hole");
            /* Error seeking, must not support sparse seek */
            return -1;
        }
hole:
        /* Is a hole, extend the file to the offset */
        ret = TEMP_FAILURE_RETRY(ftruncate(tgtfd, nextoffs));
        if (ret < 0) {
            perror("Truncate file to add hole");
            return -1;
        }

        /* Move file offset for target to after the newly added hole */
        nextoffs = TEMP_FAILURE_RETRY(lseek(tgtfd, nextoffs, SEEK_SET));
        if (nextoffs == (off_t)-1) {
            /* Something very strange happened,
               either some race condition changed the file,
               or the file is truncatable but not seekable
               or some external memory corruption,
               since EOVERFLOW can't happen with SEEK_SET */
            perror("Move to after newly added hole");
            return -1;
        }

        srcoffs = nextoffs;
    }

end_hole:
    nextoffs = TEMP_FAILURE_RETRY(lseek(srcfd, 0, SEEK_END));
    if (nextoffs == (off_t)-1) {
        perror("Seek to end of file");
        return -1;
    }
    if (srcoffs != nextoffs) {
        /* Not already at EOF, need to extend */
        int ret = TEMP_FAILURE_RETRY(ftruncate(tgtfd, nextoffs));
        if (ret < 0) {
            perror("Truncate to add hole at end of file");
            return -1;
        }
    }
    return copied;

end_data:
    {
        ssize_t ret;
        ret = naive_contents_copy(srcfd, tgtfd);
        if (ret < 0)
            return ret;
        copied += ret;
    }
    return copied;
}

static int btrfs_clone_contents(int srcfd, int tgtfd) {
        struct statfs stfs;
        struct stat st;
        int ret;

        /* Behaviour is undefined unless called on a btrfs file,
           so ensure we're calling on the right file first. */
        ret = fstatfs(tgtfd, &stfs);
        if (ret < 0)
                return ret;
        if (stfs.f_type != BTRFS_SUPER_MAGIC) {
                errno = EINVAL;
                return -1;
        }

        ret = fstat(tgtfd, &st);
        if (ret < 0)
                return ret;
        if (!S_ISREG(st.st_mode)) {
                errno = EINVAL;
                return -1;
        }

        return ioctl(tgtfd, BTRFS_IOC_CLONE, srcfd);
}

int copy_contents(int srcfd, int tgtfd) {
    int ret = -1;
    ret = btrfs_clone_contents(srcfd, tgtfd);
    if (ret >= 0)
        return ret;

    if (ret < 0 && errno != EINVAL) {
        /* Some error that wasn't from a btrfs clone,
	   so we can't fall back to something that would work */
        perror("Copy file");
        return -1;
    }

    ret = sparse_copy_contents(srcfd, tgtfd);
    if (ret >= 0)
        return ret;

    if (ret < 0 && errno != EINVAL) {
        /* Some error that wasn't from a sparse copy,
	   so we can't fall back to something that would work */
        perror("Copy file");
        return -1;
    }

    return naive_contents_copy(srcfd, tgtfd);
}
