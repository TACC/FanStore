all: prep read_remote_file

prep: 
	g++  -fPIC -Wno-unknown-pragmas -Wno-sign-compare -Wno-conversion -fomit-frame-pointer -fstrict-aliasing -ffast-math -O3 -DNDEBUG -msse4.1 lzsse8.cpp -c
	g++ -O2 -o prep prep_file.c lzsse8.o -lpthread
	chmod u+x wrapper.so

read_remote_file:
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  read_remote_file.c -I/opt/intel/compilers_and_libraries_2018.2.199/linux/mpi/intel64/include
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  dict.c
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  xxhash.c
	gcc -c -DNCX_PTR_SIZE=8 -pipe  -O3  -DLOG_LEVEL=4  -DPAGE_MERGE  ncx_slab.c
	mpicxx -o read_remote_file read_remote_file.o dict.o xxhash.o ncx_slab.o lzsse8.o
	chmod u+x wrapper.so

clean:
	rm *.o prep read_remote_file

