all: prep wrapper read_remote_file

wrapper:
	cd wrapper
	gcc -O3 -fPIC -c dict.c
	gcc -O3 -fPIC -c xxhash.c
	gcc -O3 -fPIC -c libudis86/decode.c
	gcc -O3 -fPIC -c libudis86/itab.c
	gcc -O3 -fPIC -c libudis86/syn-att.c 
	gcc -O3 -fPIC -c libudis86/syn.c  
	gcc -O3 -fPIC -c libudis86/syn-intel.c
	gcc -O3 -fPIC -c libudis86/udis86.c
	gcc -O3 -fPIC -c wrapper.c
	g++ -fPIC -shared -o ../wrapper.so wrapper.o dict.o xxhash.o decode.o itab.o syn-att.o syn.o syn-intel.o udis86.o ../lzsse8.o -ldl -lrt
	cd ..
	
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

