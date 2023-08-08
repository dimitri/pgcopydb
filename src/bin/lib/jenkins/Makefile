all: clean lookup3 test ;

test: lookup3
	./lookup3

lookup3.o: lookup3.c
	$(CC) -c -o $@ $<

lookup3: lookup3.c
	$(CC) -D SELF_TEST -Wno-implicit-fallthrough -Wno-format -o $@ $<

clean:
	rm -f lookup3.o lookup3

.PHONY: all clean test
