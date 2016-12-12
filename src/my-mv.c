
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
#include <stdbool.h>         /* bool, true, false */
#include <stdio.h>           /* perror */
#include <sys/types.h>       /* mode_t */
#include <unistd.h>          /* read, write, lseek, SEEK_{SET,CUR,DATA,HOLE}, syscall */
#include <fcntl.h>           /* open, splice */
#include <errno.h>           /* errno, E* */
#include <getopt.h>          /* getopt_long, struct option */
#include <sys/vfs.h>         /* ftatfs, struct statfs */
#include <sys/stat.h>        /* statfs, struct stat, futimens */
#include <sys/ioctl.h>       /* ioctl */
#include <libgen.h>          /* dirname */
#undef basename
#include <string.h>          /* basename */
#include <linux/fs.h>        /* FS_IOC_*_FL */
#include <strings.h>         /* ffs */
#include <stdlib.h>          /* NULL, malloc, realloc, free */
#include <sys/xattr.h>       /* flistxattr, fgetxattr, fsetxattr */
#include <selinux/selinux.h> /* freecon, setfscreatecon */
#include <selinux/label.h>   /* selabel_{open,close,lookup}, SELABEL_CTX_FILE,
                                selabel_handle */

#include "clobber.h"         /* CLOBBER_* */
#include "setgid.h"          /* SETGID_* */
#include "missing.h"         /* renameat2, RENAME_*, SEEK_*, copy_file_range */
#include "copy.h"            /* copy_contents */

static int get_flags(int fd, int *flags_out) {
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

static int set_flags(int fd, const int *flags) {
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

static int fix_owner(char *target, struct stat *source_stat, enum setgid setgid,
                     int tgtfd) {
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

static int fix_rename_owner(char *target, struct stat *source_stat,
                            enum setgid setgid) {
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

static int set_selinux_create_context(const char *tgt, mode_t srcmode) {
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

static int copy_posix_acls(int srcfd, int tgtfd) {
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

static int open_tmpfile(const char *target, char **tmpfn_out) {
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

static int copy_file(char *source, char *target, struct stat *source_stat,
                     enum clobber clobber, enum setgid setgid,
                     int required_flags) {
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

static int move_file(char *source, char *target, enum clobber clobber,
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

static void strip_trailing_slashes(char *s) {
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
