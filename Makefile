
define checkdef
`if echo 'int main(){(void)$(1);}' | \
    gcc -include stdio.h -xc - -o/dev/null 2>/dev/null; \
 then \
     echo 1; \
 else \
     echo 0; \
 fi`
endef

all: my-mv clobbering

my-mv: CFLAGS=-std=gnu99 -Wall -g -D_GNU_SOURCE -DHAVE_RENAMEAT2=$(call checkdef,renameat2) -DHAVE_COPY_FILE_RANGE=$(call checkdef,copy_file_range)
my-mv: LDLIBS=-lselinux
my-mv: src/my-mv.o
	$(CC) -o $@ $(LDFLAGS) $< $(LOADLIBES) $(LDLIBS)

clobbering: CFLAGS=-D_GNU_SOURCE -DHAVE_RENAMEAT2=$(call checkdef,renameat2)
clobbering: src/clobbering.o
	$(CC) -o $@ $(LDFLAGS) $< $(LOADLIBES) $(LDLIBS)
