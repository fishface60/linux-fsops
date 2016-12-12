
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

#include <assert.h>
#include <limits.h>          /* SSIZE_MAX */
#include <stdbool.h>         /* bool */
#include <stdio.h>           /* perror */
#include <sys/types.h>       /* off_t, ssize_t, mode_t */
#include <unistd.h>          /* read, write, lseek, SEEK_{SET,CUR,DATA,HOLE} */
#include <fcntl.h>           /* open, splice */
#include <errno.h>           /* E* */
#include <sys/syscall.h> 
#include <getopt.h>          /* getopt_long, struct option */
#include <linux/btrfs.h>     /* BTRFS_IOC_CLONE */
#include <sys/vfs.h>         /* ftatfs, struct statfs */
#include <sys/stat.h>        /* struct stat, futimens */
#include <sys/ioctl.h>       /* ioctl */
#include <linux/magic.h>     /* BTRFS_SUPER_MAGIC */
#include <sys/sendfile.h>    /* sendfile */
#include <libgen.h>          /* dirname */
#undef basename
#include <string.h>          /* basename */
#include <linux/fs.h>        /* FS_IOC_*_FL */
#include <strings.h>         /* ffs */
#include <stdlib.h>          /* malloc, realloc, free */
#include <sys/xattr.h>       /* flistxattr, fgetxattr, fsetxattr */
#include <selinux/selinux.h> /* freecon, setfscreatecon */
#include <selinux/label.h>   /* selabel_{open,close,lookup}, SELABEL_CTX_FILE,
                                selabel_handle */

#if !HAVE_DECL_RENAMEAT2
static inline int renameat2(int oldfd, const char *oldname, int newfd, const char *newname, unsigned flags) {
    return syscall(__NR_renameat2, oldfd, oldname, newfd, newname, flags);
}
#endif

#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1<<0)
#endif

#ifndef SEEK_DATA
#define SEEK_DATA 3
#endif

#ifndef SEEK_HOLE
#define SEEK_HOLE 4
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

enum setgid {
    SETGID_AUTO,
    SETGID_NEVER,
    SETGID_ALWAYS,
};

enum clobber {
    CLOBBER_PERMITTED     = 'p',
    CLOBBER_REQUIRED      = 'R',
    CLOBBER_FORBIDDEN     = 'N',
    CLOBBER_TRY_REQUIRED  = 'r',
    CLOBBER_TRY_FORBIDDEN = 'n',
};

ssize_t cfr_copy_range(int srcfd, int tgtfd, size_t range) {
    size_t to_copy = range;
    while (to_copy) {
        ssize_t ret = copy_file_range(srcfd, NULL, tgtfd, NULL, range, 0);
        if (ret < 0)
            return ret;
        to_copy -= ret;
    }
    return range;
}

ssize_t sendfile_copy_range(int srcfd, int tgtfd, size_t range) {
    size_t to_copy = range;
    while (to_copy) {
        ssize_t ret = sendfile(tgtfd, srcfd, NULL, range);
        if (ret < 0)
            return ret;
        to_copy -= ret;
    }
    return range;
}

ssize_t splice_copy_range(int srcfd, int tgtfd, size_t range) {
    size_t to_copy = range;
    while (to_copy) {
        ssize_t ret = splice(srcfd, NULL, tgtfd, NULL, range, 0);
        if (ret < 0)
            return ret;
        to_copy -= ret;
    }
    return range;
}

ssize_t naive_copy_range(int srcfd, int tgtfd, size_t range) {
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

ssize_t copy_range(int srcfd, int tgtfd, size_t range) {
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

int naive_contents_copy(int srcfd, int tgtfd) {
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

ssize_t sparse_copy_contents(int srcfd, int tgtfd) {
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

int btrfs_clone_contents(int srcfd, int tgtfd) {
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

int get_flags(int fd, int *flags_out) {
	struct stat st;
	int ret = 0;
	ret = fstat(fd, &st);
	if (ret < 0)
		return ret;
	if (!S_ISREG(st.st_mode) && !S_ISDIR(st.st_mode)
	    && !S_ISLNK(st.st_mode)) {
		errno = ENOTTY;
		return -1;
	}
	return ioctl(fd, FS_IOC_GETFLAGS, flags_out);
}

int set_flags(int fd, const int *flags) {
	struct stat st;
	int ret = 0;
	ret = fstat(fd, &st);
	if (ret < 0)
		return ret;
	if (!S_ISREG(st.st_mode) && !S_ISDIR(st.st_mode)
	    && !S_ISLNK(st.st_mode)) {
		errno = ENOTTY;
		return -1;
	}
	return ioctl(fd, FS_IOC_SETFLAGS, flags);
}

/* Update the flags of tgtfd to match srcfd.
   srcfd and tgtfd must be regular files.
   Flags are set one at a time since a filesystem may refuse to set new flags
   if any of them are invalid.
   Failure to set any flags not in required_flags is ignored.
 */
static int copy_flags(int srcfd, int tgtfd, int required_flags) {
    int ret;
    int srcflags;
    int tgtflags;
    int newflags;
    struct statfs srcfs, tgtfs;

    ret = get_flags(srcfd, &srcflags);
    if (ret != 0) {
        /* If we don't support flags we have none to update. */
        if (errno == EINVAL || errno == ENOTTY)
            return 0;
        return ret;
    }

    ret = get_flags(tgtfd, &tgtflags);
    if (ret != 0) {
        if (required_flags == 0 && (errno == EINVAL || errno == ENOTTY))
            return 0;
        return ret;
    }

    ret = fstatfs(srcfd, &srcfs);
    if (ret != 0)
        return ret;

    ret = fstatfs(tgtfd, &tgtfs);
    if (ret != 0)
        return ret;

    /* If on different fs need to mask to commonly agreed flags */
    if (srcfs.f_type != tgtfs.f_type) {
        srcflags &= FS_FL_USER_MODIFIABLE;
        tgtflags &= FS_FL_USER_MODIFIABLE;
        if ((srcflags & required_flags) != required_flags) {
            errno = EINVAL;
            return -1;
        }
    }

    /* Skip setting flags if they are the same */
    if (srcflags == tgtflags)
        return 0;

    /* Clear any flags that are set which we want to remove */
    newflags = tgtflags & srcflags;
    ret = set_flags(tgtfd, &newflags);
    if (ret != 0) {
        /* Can't set flags on the target, but we didn't require any. */
        if (required_flags == 0 && errno == EINVAL)
            return 0;
        return ret;
    }
    tgtflags = newflags;

    /* Use srcflags for flags we want to set,
       which are everything not already set. */
    srcflags &= ~tgtflags;
    while (srcflags) {
        int flag = 1 << (ffs(srcflags) - 1);

        newflags = tgtflags | flag;
        ret = set_flags(tgtfd, &newflags);
        /* Fail if this flag is required and unsettable */
        if (ret != 0 && (flag & required_flags))
            return ret;
        if (ret == 0)
            tgtflags = newflags;

        srcflags &= ~flag;
    }

    return 0;
}

static int fix_owner(char *target, struct stat *source_stat, enum setgid setgid, int tgtfd) {
    struct stat target_stat;
    struct stat dirname_stat;
    char *target_dirname = NULL;
    int ret = 0;

    if (setgid == SETGID_NEVER)
        return fchown(tgtfd, source_stat->st_uid, source_stat->st_gid);

    ret = fstat(tgtfd, &target_stat);
    if (ret < 0) {
        perror("Stat target file");
        return ret;
    }

    target_dirname = strdup(target);
    target_dirname = strcpy(target_dirname, dirname(target_dirname));
    ret = stat(target_dirname, &dirname_stat);
    if (ret < 0) {
        perror("Stat target directory");
        goto cleanup;
    }

    if ((setgid == SETGID_ALWAYS
         || (setgid == SETGID_AUTO && dirname_stat.st_gid & S_ISGID))
        && target_stat.st_gid != dirname_stat.st_gid) {
        ret = fchown(tgtfd, target_stat.st_uid, dirname_stat.st_gid);
        if (ret < 0)
            perror("Chown target");
    }

cleanup:
    free(target_dirname);
    return ret;
}

static int fix_rename_owner(char *target, struct stat *source_stat, enum setgid setgid) {
    int tgtfd = -1;
    int ret = -1;

    ret = open(target, O_RDWR);
    if (ret == -1) {
        perror("Open target file");
        goto cleanup;
    }
    tgtfd = ret;

    ret = fix_owner(target, source_stat, setgid, tgtfd);
cleanup:
    close(tgtfd);
    return ret;
}

static int realloc_double(void **buf, size_t *size) {
    size_t new_size = *size * 2;
    void *new_buf = realloc(*buf, new_size);
    if (new_buf == NULL && new_size != 0)
        return -1;
    *buf = new_buf;
    *size = new_size;
    return 0;
}

static int xattr_list(int fd, char **names, size_t *size) {
    ssize_t ret;

    if (*names == NULL && *size == 0) {
        ret = TEMP_FAILURE_RETRY(flistxattr(fd, NULL, 0));
        if (ret < 0)
            goto error;
        *size = ret;

        *names = malloc(*size);
        if (*names == NULL) {
            ret = -1;
            goto error;
        }
    }

    for (;;) {
        ret = TEMP_FAILURE_RETRY(flistxattr(fd, *names, *size));
        if (ret >= 0) {
            *size = ret;
            break;
        }

        if (errno != ERANGE)
            goto error;

        /* New xattr added since first flistxattr */
        ret = realloc_double((void**)names, size);
        if (ret < 0)
            goto error;
    }

    ret = 0;
error:
    return ret;
}

static int xattr_get(int fd, const char *name, void **value, size_t *size) {
    ssize_t ret;

    if (*value == NULL && *size == 0) {
        ret = TEMP_FAILURE_RETRY(fgetxattr(fd, name, NULL, 0));
        if (ret < 0)
            goto error;
        *size = ret;

        *value = malloc(*size);
        if (*value == NULL) {
            ret = -1;
            goto error;
        }
    }

    for (;;) {
        ret = TEMP_FAILURE_RETRY(flistxattr(fd, *value, *size));
        if (ret >= 0) {
            *size = ret;
            break;
        }

        if (errno != ERANGE)
            goto error;

        /* xattr grew since first getxattr */
        ret = realloc_double(value, size);
        if (ret < 0)
            goto error;
    }

    ret = 0;
error:
    return ret;
}

static int str_starts_with(const char *s1, const char *s2) {
    return strncmp(s1, s2, strlen(s2)) == 0;
}

static int copy_xattrs(int srcfd, int tgtfd) {
    ssize_t ret;
    char *names = NULL;
    void *value = NULL;
    size_t names_size = 0, value_size = 0;

    ret = xattr_list(srcfd, &names, &names_size);
    if (ret < 0)
        goto cleanup;

    for (char *name = names; name < names + names_size;
         name = strchrnul(name, '\0') + 1) {
        /* Skip xattrs that need special handling */
        if (!str_starts_with(name, "user.") &&
            !str_starts_with(name, "security.SMACK64") &&
            !str_starts_with(name, "btrfs.")) {
            continue;
        }

        ret = xattr_get(srcfd, name, &value, &value_size);
        if (ret < 0)
            goto cleanup;

        ret = TEMP_FAILURE_RETRY(fsetxattr(tgtfd, name, value, value_size, 0));
        if (ret < 0) {
            if (errno == EINVAL &&
                (str_starts_with(name, "security.SMACK64") ||
                 str_starts_with(name, "btrfs."))) {
                continue;
            }
            goto cleanup;
        }
    }

cleanup:
    free(names);
    free(value);
    return ret;
}

int set_selinux_create_context(const char *tgt, mode_t srcmode) {
    int ret = 0;
    struct selabel_handle *hnd = NULL;
    char *context = NULL;

    hnd = selabel_open(SELABEL_CTX_FILE, NULL, 0);
    if (hnd == NULL) {
        if (errno != ENOENT) {
            ret = 1;
        }
        goto cleanup;
    }

    ret = selabel_lookup(hnd, &context, tgt, srcmode);
    if (ret != 0) {
        goto cleanup;
    }

    ret = setfscreatecon(context);

cleanup:
    freecon(context);
    if (hnd != NULL)
        selabel_close(hnd);
    return ret;
}

int copy_posix_acls(int srcfd, int tgtfd) {
    static const char name[] = "system.posix_acl_access";
    int ret = 0;
    void *value = NULL;
    size_t size = 0;

    ret = xattr_get(srcfd, name, &value, &size);
    if (ret < 0) {
        if (errno == ENODATA)
            ret = 0;
        goto cleanup;
    }

    ret = TEMP_FAILURE_RETRY(fsetxattr(tgtfd, name, value, size, 0));
    if (ret < 0) {
        goto cleanup;
    }

cleanup:
    free(value);
    return ret;
}

int rename_file(const char *src, const char *tgt, enum clobber clobber) {
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
        case CLOBBER_PERMITTED:
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

int open_tmpfile(const char *target, char **tmpfn_out) {
    char *template = malloc(strlen(target) + sizeof("./.tmpXXXXXX"));
    char *dir = NULL;
    int ret;
    strcpy(template, target);
    dir = dirname(template);
    if (dir != template)
        strcpy(template, dir);
    strcat(template, "/");
    strcat(template, ".tmp");
    strcat(template, basename(target));
    strcat(template, "XXXXXX");
    ret = mkstemp(template);
    if (ret >= 0)
        *tmpfn_out = template;
    else
        free(template);
    return ret;
}

int copy_file(char *source, char *target, struct stat *source_stat,
              enum clobber clobber, enum setgid setgid, int required_flags) {
    int srcfd = -1;
    int tgtfd = -1;
    int ret = -1;
    char *tmppath = NULL;

    ret = open(source, O_RDONLY);
    if (ret == -1) {
        perror("Open source file");
        goto cleanup;
    }
    srcfd = ret;

    ret = set_selinux_create_context(target, source_stat->st_mode);
    if (ret != 0) {
        perror("Set selinux create context");
        goto cleanup;
    }

    ret = open_tmpfile(target, &tmppath);
    if (ret == -1) {
        perror("Open temporary target file");
        goto cleanup;
    }
    tgtfd = ret;

    ret = copy_contents(srcfd, tgtfd);
    if (ret < 0)
        goto cleanup;

    ret = fchmod(tgtfd, source_stat->st_mode);
    if (ret < 0)
        goto cleanup;

    ret = fix_owner(target, source_stat, setgid, tgtfd);
    if (ret < 0)
        goto cleanup;

    ret = copy_flags(srcfd, tgtfd, required_flags);
    if (ret < 0)
        goto cleanup;

    ret = copy_xattrs(srcfd, tgtfd);
    if (ret < 0)
        goto cleanup;

    ret = copy_posix_acls(srcfd, tgtfd);
    if (ret < 0)
        goto cleanup;

    {
        struct timespec times[] = { source_stat->st_atim, source_stat->st_mtim, };
        ret = futimens(tgtfd, times);
        if (ret < 0)
            goto cleanup;
    }

    ret = rename_file(tmppath, target, clobber);
cleanup:
    close(srcfd);
    close(tgtfd);
    if (tmppath && ret != 0)
        (void)unlink(tmppath);
    free(tmppath);
    return ret;
}

int move_file(char *source, char *target, enum clobber clobber,
              enum setgid setgid, int required_flags) {
    int ret;
    struct stat source_stat;
    bool have_source_stat = false;
    if (setgid == SETGID_NEVER) {
        ret = stat(source, &source_stat);
        if (ret < 0)
            return ret;
        have_source_stat = true;
    }

    ret = rename_file(source, target, clobber);
    if (ret == 0)
        return fix_rename_owner(target, &source_stat, setgid);
    if (errno == EXDEV)
        goto xdev;
    if (errno != ENOSYS) {
        perror("rename2");
        return ret;
    }
    /* Have to skip to copy if unimplemented since rename can't detect EEXIST */
    if (clobber == CLOBBER_FORBIDDEN)
        goto xdev;
rename:
    ret = rename(source, target);
    if (ret == 0)
        return fix_rename_owner(target, &source_stat, setgid);
    if (errno == EXDEV)
        goto xdev;
    perror("rename");
    return ret;
xdev:
    if (!have_source_stat) {
        ret = stat(source, &source_stat);
        if (ret < 0)
            return ret;
    }

    ret = copy_file(source, target, &source_stat, clobber, setgid, required_flags);
    if (ret != 0)
        return ret;
    ret = unlink(source);
    if (ret < 0)
        perror("unlink");
    return ret;
}

void strip_trailing_slashes(char *s) {
    size_t len = strlen(s);
    if (len == 0)
        return;

    for (char *end = s + len - 1; end > s; end--) {
        if (*end != '/')
            break;
        *end = '\0';
    }
}

/* Convert a chattr style flags string into flags */
static int parse_flags(const char *flagstr) {
    static struct flags_char {
    	int flag;
        char flagchar;
    } flags_chars[] = {
        { FS_SECRM_FL, 's' },
        { FS_UNRM_FL, 'u' },
        { FS_COMPR_FL, 'c' },
        { FS_SYNC_FL, 'S' },
        { FS_IMMUTABLE_FL, 'i' },
        { FS_APPEND_FL, 'a' },
        { FS_NODUMP_FL, 'd' },
        { FS_NOATIME_FL, 'A' },
        { FS_JOURNAL_DATA_FL, 'j' },
        { FS_NOTAIL_FL, 't' },
        { FS_DIRSYNC_FL, 'D' },
        { FS_TOPDIR_FL, 'T' },
#ifdef FS_EXTENTS_FL
        { FS_EXTENTS_FL, 'e'},
#endif
        { FS_NOCOW_FL, 'C' },
#ifdef FS_PROJINHERIT_FL
        { FS_PROJINHERIT_FL, 'P' },
#endif
    };
    int flags = 0;

    for (int i = 0; i < (sizeof flags_chars / sizeof *flags_chars); i++) {
        if (strchr(flagstr, flags_chars[i].flagchar))
            flags |= flags_chars[i].flag;
    }

    return flags;
}

int main(int argc, char *argv[]) {
    char *source;
    char *target;
    enum clobber clobber = CLOBBER_PERMITTED;
    enum setgid setgid = SETGID_AUTO;
    int required_flags = 0;

    enum opt {
        OPT_CLOBBER_PERMITTED     = 'p',
        OPT_CLOBBER_REQUIRED      = 'R',
        OPT_CLOBBER_FORBIDDEN     = 'N',
        OPT_CLOBBER_TRY_REQUIRED  = 'r',
        OPT_CLOBBER_TRY_FORBIDDEN = 'n',
        OPT_NO_SETGID = 'G',
        OPT_SETGID = 'g',
        OPT_FLAGS = 'f',
    };
    static const struct option opts[] = {
        { .name = "clobber-permitted",     .has_arg = no_argument,
          .val = OPT_CLOBBER_PERMITTED, },
        { .name = "clobber-required",      .has_arg = no_argument,
          .val = OPT_CLOBBER_REQUIRED, },
        { .name = "try-clobber-required",  .has_arg = no_argument,
          .val = OPT_CLOBBER_TRY_REQUIRED, },
        { .name = "clobber-forbidden",     .has_arg = no_argument,
          .val = OPT_CLOBBER_FORBIDDEN, },
        { .name = "try-clobber-forbidden", .has_arg = no_argument,
          .val = OPT_CLOBBER_TRY_FORBIDDEN, },
        { .name = "no-setgid",             .has_arg = no_argument,
          .val = OPT_NO_SETGID, },
        { .name = "setgid",                .has_arg = no_argument,
          .val = OPT_SETGID, },
        { .name = "required-flags",        .has_arg = required_argument,
          .val = OPT_FLAGS, },
        {},
    };

    for (;;) {
        int ret = getopt_long(argc, argv, "pRNrnGgf:", opts, NULL);
        if (ret == -1)
            break;
        switch (ret) {
        case '?':
            return 1;
        case OPT_CLOBBER_PERMITTED:
        case OPT_CLOBBER_REQUIRED:
        case OPT_CLOBBER_FORBIDDEN:
        case OPT_CLOBBER_TRY_REQUIRED:
        case OPT_CLOBBER_TRY_FORBIDDEN:
            clobber = ret;
            break;
        case OPT_NO_SETGID:
            setgid = SETGID_NEVER;
            break;
        case OPT_SETGID:
            setgid = SETGID_ALWAYS;
            break;
        case OPT_FLAGS:
            required_flags = parse_flags(optarg);
            break;
        }
    }
    if (optind == argc || argc > optind + 2) {
        fprintf(stderr, "1 or 2 positional arguments required\n");
        return 2;
    }

    source = argv[optind];
    strip_trailing_slashes(source);

    if (optind + 2 == argc) {
        target = argv[optind + 1];
        strip_trailing_slashes(target);
    } else {
        target = basename(source);
        /* Returns NULL if source ends with / */
        if (target == NULL)
            target = source;
    }

    if (move_file(source, target, clobber, setgid, required_flags) >= 0)
        return 0;
    return 1;   
}
