OPTIMIZE := -O2
WARNINGS := -Wall 
EXTRA_CFLAGS := -g -std=c++0x -DDEBUG=0 -D_FILE_OFFSET_BITS=64 

############################################

CURRENT = $(CURDIR)
SERVER_TEST_DIR=			src/server/unit_test

default: server_test

server_test:
	make OPTIMIZE="$(OPTIMIZE)" WARNINGS="$(WARNINGS)" EXTRA_CFLAGS="$(EXTRA_CFLAGS)" -C ${SERVER_TEST_DIR}

clean:
	make -C ${SERVER_TEST_DIR} clean
