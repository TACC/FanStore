all: prep read_remote_file lib

lib:
	gcc -O3 -fPIC -c wrapper/dict.c -o wrapper/dict.o
	gcc -O3 -fPIC -c wrapper/xxhash.c -o wrapper/xxhash.o
	gcc -O3 -fPIC -c wrapper/libudis86/decode.c -o wrapper/decode.o
	gcc -O3 -fPIC -c ${PWD}/wrapper/libudis86/itab.c -o wrapper/itab.o
	gcc -O3 -fPIC -c ${PWD}/wrapper/libudis86/syn-att.c -o wrapper/syn-att.o
	gcc -O3 -fPIC -c ${PWD}/wrapper/libudis86/syn.c -o wrapper/syn.o 
	gcc -O3 -fPIC -c ${PWD}/wrapper/libudis86/syn-intel.c -o wrapper/syn-intel.o
	gcc -O3 -fPIC -c ${PWD}/wrapper/libudis86/udis86.c -o wrapper/udis86.o
	gcc -O3 -fPIC -c wrapper/wrapper.c -o wrapper/wrapper.o
	g++ -fPIC -shared -o wrapper.so wrapper/wrapper.o wrapper/dict.o wrapper/xxhash.o wrapper/decode.o wrapper/itab.o wrapper/syn-att.o wrapper/syn.o wrapper/syn-intel.o wrapper/udis86.o lzsse8.o -ldl -lrt

prep: 
	g++  -fPIC -Wno-unknown-pragmas -Wno-sign-compare -Wno-conversion -fomit-frame-pointer -fstrict-aliasing -ffast-math -O3 -DNDEBUG -msse4.1 lzsse8.cpp -c
	g++ -O2 -o prep prep_file.c lzsse8.o -lpthread

read_remote_file:
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  read_remote_file.c -I/opt/intel/compilers_and_libraries_2018.2.199/linux/mpi/intel64/include
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  dict.c
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  xxhash.c
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  ncx_slab.c
	mpicxx -o read_remote_file read_remote_file.o dict.o xxhash.o ncx_slab.o lzsse8.o

clean:
	rm *.o prep read_remote_file wrapper.so

