#define __USE_GNU

#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <mpi.h>

#define SIZE (1024*1024*16)
//#define NFILE (500)
//#define NFILE (80)
#define NFILE (1024*1024)

int nFile=0, nFileWithData=0;

unsigned char szBuff[SIZE];
long int nByteRead=0;
char szNameList[NFILE][256];
char szDirPrefix[256];

int rank, gsize;


void Read_File(int i)
{
        int fd, num_read=0;
        char szName[256];

        if(szDirPrefix[0])      {
                sprintf(szName, "%s/%s", szDirPrefix, szNameList[i]);
        }
        else    {
                sprintf(szName, "%s", szNameList[i]);
        }

        fd = open(szName, O_RDONLY, 0);
        if(fd == -1)    {
		printf("Fail to open file %s\n", szName);
		return;
	}
        num_read = read(fd, szBuff, SIZE);
        nByteRead += num_read;
        close(fd);

        if(num_read > 0) nFileWithData++;
}

int main(int argc, char *argv[])
{
        int i, nLen, nFilePerNode;
    static struct timeval tm1, tm2;

        FILE *fIn;

        if(argc < 2)    {
                printf("Usage: read_file_mpi filelist [path_prefix]\n");
                exit(1);
        }

        fIn = fopen(argv[1], "r");
        if(fIn == NULL) {
                printf("Fail to open file %s\nQuit\n", argv[1]);
                exit(1);
        }

        if(argc == 3)   {
                strcpy(szDirPrefix, argv[2]);
        }
        else    {
                szDirPrefix[0] = 0;     // null string
        }

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); //get the rank or ID of the current process
    MPI_Comm_size(MPI_COMM_WORLD, &gsize); //number of processes that are running

        for(i=0; i<NFILE; i++)  {
                fgets(szNameList[i], 256, fIn);
                nLen = strlen(szNameList[i]);
                if(nLen < 3)    {
                        break;
                }
                if(szNameList[i][nLen - 1] == 0xA)      {
                        szNameList[i][nLen - 1] = 0;
                }
                nFile++;
        }
        fclose(fIn);

    gettimeofday(&tm1, NULL);

    if(nFile % gsize == 0) {
        nFilePerNode = nFile/gsize;
    }
    else {
        nFilePerNode = nFile/gsize + 1;
    }


    for(i=0; i<nFile; i++) {
        Read_File(  (i+(rank*nFilePerNode)) % nFile  );
//        if(i%5000 == 0) printf("Finished %d files.\n", i);
    }

    gettimeofday(&tm2, NULL);

        unsigned long long t = 1000000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec);
        printf("Rank %d, Time %llu us. %ld bytes in %d files. Speed %6.1lf MB/s.\n", rank, t, nByteRead, nFileWithData, 1.0*nByteRead/t);

        MPI_Finalize();


        return 0;
}

