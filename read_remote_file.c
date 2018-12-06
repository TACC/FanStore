#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <string.h>
#include <sys/types.h>
#include <mpi.h>
#include <sys/mman.h>
#include <pthread.h>
#include <sys/stat.h>
#include <errno.h>
#include "lzsse8.h"

#include "dict.h"
#include "ncx_slab.h"

#define SIZE (1024*1024*16)
#define ID_SERVER	(0)
#define ID_CLIENT	(1)

#define MAX_LEN_FILE_NAME	(108)	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#define MAX_DIR		(128*1024)
#define MAX_FILE	(2*1024*1024)
#define LEN_REC		(320)
#define MAX_PARTITION	(2048)
#define MAX_FILE_SIZE	(100*1024*1024)
#define MAX_DIR_ENTRY_LEN	(64)

#define MSG_TAG_REQUESTFILE	(100)
#define MSG_TAG_FILECONTENT	(101)

#define QUEUE_SIZE		(4096)
#define QUEUE_FULL		((QUEUE_SIZE) - 1)
#define QUEUE_TAG_OPEN	(0x1)	// tag is stored as the two highest bits.
#define QUEUE_TAG_CLOSE	(0x2)	// tag is stored as the two highest bits.
#define QUEUE_TAG_DONE	(0x3)	// Expected operation is DONE already.

#define PTHREAD_MUTEXATTR_FLAG_PSHARED (0x80000000)	// int 

static int bDEBUG=0;
static int bUsePack=0;

static void *p_shm_ht=NULL;	// ptr to shared memory
static int shm_ht_fd;	//
static char szNameHTShm[]="fs_ht_shm";
static int nSize_Shared_Memory_HT=0;

static void *p_shm_ht_dir=NULL;	// ptr to shared memory
static int shm_ht_dir_fd;	//
static char szNameHTDirShm[]="fs_ht_dir_shm";
static int nSize_Shared_Memory_HT_Dir=0;

static void *p_shm_queue=NULL;	// ptr to shared memory
static int shm_queue_fd;	//
static char szNameQueueShm[]="fs_queue_shm";
static int nSize_Shared_Memory_Queue=0;

static void *p_shm_meta=NULL;	// ptr to shared memory
static int shm_meta_fd;	//
static char szNameMetaShm[]="fs_meta_shm";
static int nSize_Shared_Memory_Meta=0;
static int *p_nFile;
static int *p_myrank;
static int *p_bUsePack;

static void *p_shm_file_buff=NULL;	// ptr to shared memory
static int shm_file_buff_fd;	//
static char szNameFileBuffShm[]="fs_buff_shm";
static int nSize_Shared_Memory_File_Buff=0;


ncx_slab_pool_t *sp_local, *sp_remote;

size_t 	pool_size_local=256*1024*1024;

u_char 	*space_local, *space_remote;

//pthread_spinlock_t mempool_lock;

//start data related with file opeeration queue
long int *Queued_Job_List;
pthread_mutex_t *p_queue_lock;
long int *p_front=NULL, *p_back=NULL;
//Given an array A of a default size (. 1) with two references back and front,
//originally set to -1 and 0 respectively. Each time we insert (enqueue) a new item,
//we increase the back index; when we remove (dequeue) an item - we increase the front index.
void enqueue(void);
long int dequeue(void);
//end data related with file opeeration queue

char szRoot[256]="/tmp/fs";
int uid;
char szFilePath[256];	// where files fs_x stored.

unsigned char szBuff[SIZE];
int nByteRead=0;
int nLen_stat, nLen_File_Name;
int nFile=0, nFileLocal=0, nFile_Bcast=0, nPartition=1;
int nDir=0;

int rank, gsize;

Dict p_Hash_File;
struct elt *elt_list_file;
int *ht_table_file=NULL;

Dict p_Hash_Dir;
struct elt *elt_list_dir;
int *ht_table_dir=NULL;

char szDirInfoFileName[256]="";
char szFileLocal[256];

#define FILE_NAME_LEN_in_META_DATA	(216)

typedef struct {
	int idx_fs;	// index of node where stores current file
	int nOpen;	// reference counter. The number of being openned by all processes on one node
	long int addr;	// the beginning address of this file in the container
	long int size;	// size of current file
	long int size_packed;	// size of packed file
	long int pData;	// the pointer to file content if open is open and readed
	char szName[FILE_NAME_LEN_in_META_DATA];	// adjusted to make the size of FILEREC is 256 bytes.
//	struct stat st;
}FILEREC, *PFILEREC;

typedef struct	{
	int nEntry;
	int addr;
	char szDirName[168];	// MAX_LEN_FILE
}DIRREC;

FILEREC *p_MetaData;	// the array of meta data. Same on all nodes.
struct stat *p_stat;	// the array of stat info of all files

static DIRREC *p_DirRec;
static unsigned char *p_DirInfo;
static long int nDirInfoFileSize=0;
static int nMoreNodeData=0;	// the extra number of nodes' data will be stored locally
signed int nMoreFilesToStore=0, nMoreFilesToStoreStart=0, nMoreFilesToStoreReal=0;

static void Create_All_Dir(void);
static void Write_Files(void);

static void Setup_Hash_Table(void);

static unsigned char *Read_Remote_File(int file_idx);
static unsigned char *Read_Remote_Packed_File(int file_idx);
static void *Handle_Remote_Access(void *void_ptr);
static void *Handle_Open_Close_File_Queue(void *void_ptr);	// only handl open, close. Once one is called, read the whole file into buffer.
static void init_shm(void);
static void Broadcast_Shared_Files(void);
static void Query_DirInfo_File_Size(void);
static void Broadcast_DirInfo_and_Setup_Hash_Table(void);
static void Store_More_Files_Locally(void);

#define OUT_OF_SPACE	(-111)

ssize_t read_all(int fd, void *buf, size_t count);
ssize_t write_all(int fd, const void *buf, size_t count);

unsigned long int rdtscp(void)
{
    unsigned long int rax, rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=d" (rdx) : : );
    return (rdx << 32) + rax;
}


ssize_t read_all(int fd, void *buf, size_t count)
{
	ssize_t ret, nBytes=0;

	while (count != 0 && (ret = read(fd, buf, count)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)
				continue;
			perror ("read");
			break;
		}
		nBytes += ret;
		count -= ret;
		buf += ret;
	}
	return nBytes;
}

ssize_t write_all(int fd, const void *buf, size_t count)
{
	ssize_t ret, nBytes=0;
	void *p_buf;

	p_buf = (void *)buf;
	while (count != 0 && (ret = write(fd, p_buf, count)) != 0) {
		if (ret == -1) {
			if (errno == EINTR)	{
				continue;
			}
			else if (errno == ENOSPC)	{	// out of space. Quit immediately!!!
				return OUT_OF_SPACE;
			}

			perror ("write");
			break;
		}
		nBytes += ret;
		count -= ret;
		p_buf += ret;
	}
	return nBytes;
}

int main(int argc, char ** argv)
{
	char szUid[16];
	int required=MPI_THREAD_MULTIPLE, provided;
	int i, nLen;
    static struct timeval tm1, tm2;
	FILE *fIn;
	pthread_t thread_Handle_Remote_Access;
	pthread_t thread_Handle_Open_Close_File_Queue;

	if(argc < 3)	{
		printf("Usage: fs n_partition szFilePath\nQuit\n");
		exit(1);
	}
	nPartition = atoi(argv[1]);
	strcpy(szFilePath, argv[2]);
	if(argc == 4)	{
		nMoreNodeData = atoi(argv[3]);
	}
	else	{
		nMoreNodeData = 0;
	}

	nLen_stat = sizeof(struct stat);
	nLen_File_Name = LEN_REC - nLen_stat - sizeof(long int);

	uid = getuid();
	sprintf(szUid, "_%d", uid);
	strcat(szRoot, szUid);	// set up root dir

	char *szEnv_fs_Root;
	int nLenRootDir;
	szEnv_fs_Root=getenv("FS_ROOT");
	if(szEnv_fs_Root != NULL)	{
		strncpy(szRoot, szEnv_fs_Root, 128);
		nLenRootDir = strlen(szRoot); 
		if(szRoot[nLenRootDir-1] == '/')	{
			szRoot[nLenRootDir-1] = 0;	// remove the last '/'
		}
	}


	Create_All_Dir();

    MPI_Init_thread(&argc, &argv, required, &provided);
	if(provided < required)	{
		printf("Fatal error. Lack of support for MPI_THREAD_MULTIPLE.\nQuit\n");
		exit(1);
	}

    MPI_Comm_rank(MPI_COMM_WORLD, &rank); //get the rank or ID of the current process
    MPI_Comm_size(MPI_COMM_WORLD, &gsize); //number of processes that are running

	Query_DirInfo_File_Size();
	init_shm();
	Broadcast_DirInfo_and_Setup_Hash_Table();
	Write_Files();
	if(rank == 0) printf("Finished preparing %d scatter files.\n", nFile);

	Setup_Hash_Table();

//	printf("rank %d, before Broadcast_Shared_Files.\n", rank);

	Broadcast_Shared_Files();

//	printf("rank %d, after Broadcast_Shared_Files.\n", rank);

	if( (rank == 0) && (nFile_Bcast > 0) ) printf("Finished preparing %d shared files on all nodes.\n", nFile_Bcast);


	sp_local = (ncx_slab_pool_t*) space_local;
	sp_local->addr = space_local;
	sp_local->min_shift = 3;
	sp_local->end = space_local + pool_size_local;
	ncx_slab_init(sp_local);

	space_remote = (u_char *)malloc(pool_size_local);
	sp_remote = (ncx_slab_pool_t*) space_remote;
	sp_remote->addr = space_remote;
	sp_remote->min_shift = 3;
	sp_remote->end = space_remote + pool_size_local;
	ncx_slab_init(sp_remote);

//	printf("rank %d, before MPI_Barrier.\n", rank);

//	for(i=0; i<nFile; i++) {
//		p_MetaData[i].nOpen = 0;
//	}

	MPI_Barrier(MPI_COMM_WORLD);

//	printf("rank %d, after MPI_Barrier.\n", rank);
	
	if(pthread_create(&(thread_Handle_Remote_Access), NULL, Handle_Remote_Access, NULL)) {
		fprintf(stderr, "Error creating thread\n");
		return 1;
	}

//	printf("rank %d, after pthread_create.\n", rank);

	Store_More_Files_Locally();
	MPI_Barrier(MPI_COMM_WORLD);
	if(rank == 0) printf("Finished storing extra %d files locally.\nReady.\n", nMoreFilesToStoreReal);

	if(pthread_create(&(thread_Handle_Open_Close_File_Queue), NULL, Handle_Open_Close_File_Queue, NULL)) {
		fprintf(stderr, "Error creating thread\n");
		return 1;
	}
	
	
	if(pthread_join(thread_Handle_Remote_Access, NULL)) {
		fprintf(stderr, "Error joining thread %d\n", i);
		return 2;
	}

	if(pthread_join(thread_Handle_Open_Close_File_Queue, NULL)) {
		fprintf(stderr, "Error joining thread %d\n", i);
		return 3;
	}

	free(space_remote);

	MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();

	return 0;
}

static void init_shm(void)
{
	shm_ht_fd = shm_open(szNameHTShm, O_RDWR | O_CREAT, 0600);
	if(shm_ht_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameHTShm);
		exit(1);
	}

	nSize_Shared_Memory_HT = sizeof(struct dict) + sizeof(int)*INITIAL_SIZE + sizeof(struct elt)*INITIAL_SIZE; // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	if (ftruncate(shm_ht_fd, nSize_Shared_Memory_HT) != 0) {
		perror("ftruncate for shm_ht_fd");
	}
	
	p_shm_ht = mmap(NULL, nSize_Shared_Memory_HT, PROT_READ | PROT_WRITE, MAP_SHARED, shm_ht_fd, 0);
	if (p_shm_ht == MAP_FAILED) {
		perror("mmap failed for p_shm_ht");
	}
	
	p_Hash_File = (struct dict *)p_shm_ht;
	DictCreate(p_Hash_File, 1024*1024*2, &elt_list_file, &ht_table_file);	// init hash table



	// start dir info related data
	shm_ht_dir_fd = shm_open(szNameHTDirShm, O_RDWR | O_CREAT, 0600);
	if(shm_ht_dir_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameHTDirShm);
		exit(1);
	}

	nSize_Shared_Memory_HT_Dir = sizeof(struct dict) + sizeof(int)*nDir*2 + sizeof(struct elt)*nDir*2 + sizeof(DIRREC)*nDir + nDirInfoFileSize;
	if (ftruncate(shm_ht_dir_fd, nSize_Shared_Memory_HT_Dir) != 0) {
		perror("ftruncate for shm_ht_dir_fd");
	}
	
	p_shm_ht_dir = mmap(NULL, nSize_Shared_Memory_HT_Dir, PROT_READ | PROT_WRITE, MAP_SHARED, shm_ht_dir_fd, 0);
	if (p_shm_ht_dir == MAP_FAILED) {
		perror("mmap failed for p_shm_ht_dir");
	}
	
	p_Hash_Dir = (struct dict *)p_shm_ht_dir;
	DictCreate(p_Hash_Dir, nDir*2, &elt_list_dir, &ht_table_dir);	// init hash table

	p_DirRec = (DIRREC *)(p_shm_ht_dir + sizeof(struct dict) + sizeof(int)*nDir*2 + sizeof(struct elt)*nDir*2);
	p_DirInfo = (unsigned char *)(p_shm_ht_dir + sizeof(struct dict) + sizeof(int)*nDir*2 + sizeof(struct elt)*nDir*2 + sizeof(DIRREC)*nDir);


	// start the shared memory for io job queue
	shm_queue_fd = shm_open(szNameQueueShm, O_RDWR | O_CREAT, 0600);
	if(shm_queue_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameQueueShm);
		exit(1);
	}

	nSize_Shared_Memory_Queue = sizeof(pthread_mutex_t) + sizeof(long int)*2 + sizeof(int)*1 + sizeof(long int)*QUEUE_SIZE;
	if (ftruncate(shm_queue_fd, nSize_Shared_Memory_Queue) != 0) {
		perror("ftruncate for shm_queue_fd");
	}
	
	p_shm_queue = mmap(NULL, nSize_Shared_Memory_Queue, PROT_READ | PROT_WRITE, MAP_SHARED, shm_queue_fd, 0);
	if (p_shm_queue == MAP_FAILED) {
		perror("mmap failed for p_shm_queue");
	}

	p_queue_lock = (pthread_mutex_t *)p_shm_queue;
	p_front = (long int *)(p_shm_queue + sizeof(pthread_mutex_t) );
	p_back =  (long int *)(p_shm_queue + sizeof(pthread_mutex_t) + sizeof(long int));
	Queued_Job_List = (long int *)(p_shm_queue + sizeof(pthread_mutex_t) + sizeof(long int)*2);

	int *p_mutex_attr;
	pthread_mutexattr_t mattr;
	p_mutex_attr = (int *)(&mattr);
	*p_mutex_attr = PTHREAD_MUTEXATTR_FLAG_PSHARED;	// PTHREAD_PROCESS_SHARED !!!!!!!!!!!!!!! Shared between processes
	if(pthread_mutex_init(p_queue_lock, &mattr) != 0) {
//  if(pthread_mutex_init(&mut, NULL) != 0) {
        perror("pthread_mutex_init");
        exit(1);
	}
	// end   the shared memory for io job queue

	// start the shared memory for meta data info
	shm_meta_fd = shm_open(szNameMetaShm, O_RDWR | O_CREAT, 0600);
	if(shm_meta_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameMetaShm);
		exit(1);
	}

	nSize_Shared_Memory_Meta = 16 + sizeof(FILEREC)*MAX_FILE + sizeof(struct stat)*MAX_FILE;
	if (ftruncate(shm_meta_fd, nSize_Shared_Memory_Meta) != 0) {
		perror("ftruncate for shm_meta_fd");
	}
	
	p_shm_meta = mmap(NULL, nSize_Shared_Memory_Meta, PROT_READ | PROT_WRITE, MAP_SHARED, shm_meta_fd, 0);
	if (p_shm_meta == MAP_FAILED) {
		perror("mmap failed for p_shm_meta");
	}

	p_nFile = (int *) p_shm_meta;
	p_myrank = (int *) (p_shm_meta + sizeof(int));	*p_myrank = rank;
	p_bUsePack = (int *) (p_shm_meta + sizeof(int)*2); // *p_bUsePack = bUsePack;
	p_MetaData = (FILEREC *) (p_shm_meta + 16);
	p_stat = (struct stat *) (p_shm_meta + 16 + sizeof(FILEREC)*MAX_FILE);
	// end the shared memory for meta data info

	// start the shared memory for file buffer
	shm_file_buff_fd = shm_open(szNameFileBuffShm, O_RDWR | O_CREAT, 0600);
	if(shm_file_buff_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameFileBuffShm);
		exit(1);
	}

	nSize_Shared_Memory_File_Buff = sizeof(char)*pool_size_local;
	if (ftruncate(shm_file_buff_fd, nSize_Shared_Memory_File_Buff) != 0) {
		perror("ftruncate for shm_file_buff_fd");
	}
	
	p_shm_file_buff = mmap(NULL, nSize_Shared_Memory_File_Buff, PROT_READ | PROT_WRITE, MAP_SHARED, shm_file_buff_fd, 0);
	if (p_shm_file_buff == MAP_FAILED) {
		perror("mmap failed for p_shm_meta");
	}

	space_local = (char *) p_shm_file_buff;
//	printf("rank = %d space_local = %p\n", rank, space_local);
	// end   the shared memory for file buffer
	
}


static void Create_All_Dir(void)
{
	FILE *fIn;
	char *ReadLine, szLine[256], szCmd[256], szName[256];
	int nLen;

	sprintf(szCmd, "mkdir -p %s &> /dev/null", szRoot);
	system(szCmd);

	sprintf(szName, "%s/dir.list", szFilePath);
	fIn = fopen(szName, "r");
	if(fIn == NULL)	{
		printf("Fail to open file: %s\nQuit.\n", szName);
	}
	while(1)	{
		if(feof(fIn))	{
			break;
		}
		ReadLine = fgets(szLine, 256, fIn);
		if(ReadLine == NULL)	{
			break;
		}
		nLen = strlen(szLine);
		if(szLine[nLen-1] == 0xA)	{
			szLine[nLen-1] = 0;
			nLen--;
		}
		if(szLine[nLen-2] == 0xD)	{
			szLine[nLen-2] = 0;
			nLen--;
		}

		sprintf(szCmd, "mkdir -p %s/%s &> /dev/null", szRoot, szLine);
		system(szCmd);
	}
}


static void Write_Files(void)
{
	int fd_in, fd_out, i, j, nReadBytes, nWriteBytes, nfile_in_this_partition, nFileLocal_Max=0, ret;
	char szNameIn[256], szNameOut[256], *szNameList_local=NULL;
	FILEREC *p_filerec_local=NULL;
	struct stat *p_stat_local=NULL;
	unsigned char *pBuff;
	int *p_nFileLocalList=NULL, *displs, *recvcounts;
	long int nBytesAllFiles=0, nBytesPacked_Sum=0, nBytesPacked, nBytes_to_Read;
	struct stat file_stat;

	nFileLocal = 0;
	pBuff = malloc(MAX_FILE_SIZE);
	if(!pBuff)	{
		printf("Fail to allocate memory for pBuff.\nQuit\n");
		exit(1);
	}

	for(i=0; i<nPartition; i++)	{
		if( i % gsize == rank )	{
			sprintf(szNameIn, "%s/fs_%d", szFilePath, i);
			fd_in = open(szNameIn, O_RDONLY);
			if(fd_in == -1)	{
				printf("Fail to open file %s to read.\nQuit\n", szNameIn);
				exit(1);
			}

			nReadBytes = read_all(fd_in, &nfile_in_this_partition, sizeof(int));
			if(nReadBytes != sizeof(int))	{
				printf("Error in reading file %s\nQuit\n", szNameIn);
				exit(1);
			}
			if(p_filerec_local == NULL)	{
				nFileLocal_Max = nfile_in_this_partition*(nPartition/gsize + 1);
				p_filerec_local = (FILEREC *) malloc(nFileLocal_Max*sizeof(FILEREC));
				p_stat_local = (struct stat *) malloc(nFileLocal_Max*sizeof(struct stat));

				memset(p_filerec_local, 0, nFileLocal_Max*sizeof(FILEREC));
				memset(p_stat_local, 0, nFileLocal_Max*sizeof(struct stat));
			}

			for(j=0; j<nfile_in_this_partition; j++)	{
				nReadBytes = read_all(fd_in, &(p_filerec_local[nFileLocal].szName), nLen_File_Name);
				if(nReadBytes != nLen_File_Name)	{
					printf("Error in reading file %s. nReadBytes != nLen_File_Name\nQuit\n", szNameIn);
					exit(1);
				}
				if(p_filerec_local[nFileLocal].szName[FILE_NAME_LEN_in_META_DATA-1])	{	// overwritten
					printf("Please check the length of file name: %s\nOverflow is detected.\nQuit.\n", p_filerec_local[nFileLocal].szName);
					exit(1);
				}
				nReadBytes = read_all(fd_in, &(p_stat_local[nFileLocal]), nLen_stat);
				if(nReadBytes != nLen_stat)	{
					printf("Error in reading file %s. nReadBytes != nLen_stat\nQuit\n", szNameIn);
					exit(1);
				}
				p_filerec_local[nFileLocal].addr = nBytesAllFiles;
				p_filerec_local[nFileLocal].size = p_stat_local[nFileLocal].st_size;
				p_filerec_local[nFileLocal].idx_fs = rank;

				nReadBytes = read_all(fd_in, &nBytesPacked, sizeof(long int));
				
				nBytesPacked_Sum += nBytesPacked;	// == 0 means unpacked
				
				if(nBytesPacked != p_stat_local[nFileLocal].st_size) {
					if(nBytesPacked == 0) { // read stat.st_size
						nBytes_to_Read = p_stat_local[nFileLocal].st_size;
					}
					else { // read nBytesPacked
						nBytes_to_Read = nBytesPacked;
					}
				}
				else {	// read stat.st_size
					nBytes_to_Read = p_stat_local[nFileLocal].st_size;
				}

				if(nBytesPacked != 0)	{
					p_filerec_local[nFileLocal].size_packed = nBytes_to_Read;
				}
				else	{
					p_filerec_local[nFileLocal].size_packed = 0;	// This file is NOT packed. 
				}
				
				nReadBytes = read_all(fd_in, pBuff, nBytes_to_Read);
				if(nReadBytes != nBytes_to_Read)	{
					printf("Error in reading file %s. nReadBytes != nBytes_to_Read\nQuit\n", szNameIn);
					exit(1);
				}

				sprintf(szNameOut, "%s/%s", szRoot, p_filerec_local[nFileLocal].szName);
				fd_out = open(szNameOut, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
				if(fd_out == -1)	{
					ret = stat(szNameOut, &file_stat);
					if( (ret == 0) && ((file_stat.st_mode & S_IFMT) == S_IFDIR) )	{	// Existing dir. Expected behavior.
					}
					else	{
						printf("Rank %d: Fail to open file %s\nQuit\n", rank, szNameOut);
						exit(1);
					}
				}
				else	{
					nWriteBytes = write_all(fd_out, pBuff, nBytes_to_Read);

					if(nWriteBytes != nBytes_to_Read)	{
						printf("Error in writing file for %s. nWriteBytes (%d) != nBytes_to_Read (%d)\nQuit\n", szNameOut, nWriteBytes, nBytes_to_Read);
						exit(1);
					}
					close(fd_out);
//					printf("Rank = %d Write file %s, size_packed = %d\n", rank, p_filerec_local[nFileLocal].szName, p_filerec_local[nFileLocal].size_packed);
					nBytesAllFiles += p_filerec_local[nFileLocal].size;
				}
				nFileLocal++;

				if( (rank == 0) && (nFileLocal % 20000 == 0)) printf("Rank = 0, copied %d files\n", nFileLocal);

			}

			close(fd_in);
		}
	}

	if(nBytesPacked_Sum == 0)	{
		bUsePack = 0;
	}
	else	{
		bUsePack = 1;
	}
	*p_bUsePack = bUsePack;
	
	strcpy(szFileLocal, szNameOut);

	if(pBuff)	free(pBuff);

	p_nFileLocalList = (int *)malloc(sizeof(int)*gsize);
	displs = (int *)malloc(sizeof(int)*gsize);
	recvcounts = (int *)malloc(sizeof(int)*gsize);

	MPI_Allgather(&nFileLocal, 1, MPI_INT, p_nFileLocalList, 1, MPI_INT, MPI_COMM_WORLD);

	nFile = 0;
	for(i=0; i<gsize; i++)	{
		nFile += p_nFileLocalList[i];
	}

	if( (rank == 0) && (p_nFileLocalList[gsize-1] > p_nFileLocalList[0]) )	{
		printf("Warning: p_nFileLocalList[gsize-1] > p_nFileLocalList[0]. %d %d\n", p_nFileLocalList[gsize-1], p_nFileLocalList[0]);
	}

	*p_nFile = nFile;

	if(nFile > MAX_FILE)	{
		printf("nFile = %d. MAX_FILE = %d\nPlease increase MAX_FILE.\nQuit\n", nFile, MAX_FILE);
		exit(1);
	}

	for(i=0; i<gsize; i++)	{
		recvcounts[i] = sizeof(FILEREC)*p_nFileLocalList[0];
	}
	displs[0] = 0;
	for(i=1; i<gsize; i++)	{
		displs[i] = displs[i-1] + recvcounts[i-1];
	}
	nMoreFilesToStoreStart = displs[rank]/sizeof(FILEREC) + nFileLocal;

	MPI_Allgatherv(p_filerec_local, sizeof(FILEREC)*nFileLocal, MPI_CHAR, p_MetaData, recvcounts, displs, MPI_CHAR, MPI_COMM_WORLD);


	for(i=0; i<gsize; i++)	{
//		recvcounts[i] = sizeof(struct stat)*nFileLocal;
		recvcounts[i] = sizeof(struct stat)*p_nFileLocalList[0];
	}
	displs[0] = 0;
	for(i=1; i<gsize; i++)	{
		displs[i] = displs[i-1] + recvcounts[i-1];
	}

	MPI_Allgatherv(p_stat_local, sizeof(struct stat)*nFileLocal, MPI_CHAR, p_stat, recvcounts, displs, MPI_CHAR, MPI_COMM_WORLD);


	if(nFileLocal < nFile)	{
		nMoreFilesToStore = p_nFileLocalList[0] * nMoreNodeData;
	}
	else	{
		nMoreFilesToStore = 0;
	}
//	printf("rank = %d nFileLocal = %d nMoreFilesToStore = %d nMoreFilesToStoreStart = %d \n", rank, nFileLocal, nMoreFilesToStore, nMoreFilesToStoreStart);


	if(p_nFileLocalList)	free(p_nFileLocalList);
	if(displs)	free(displs);
	if(recvcounts)	free(recvcounts);

	if(p_filerec_local)	free(p_filerec_local);
	if(p_stat_local)	free(p_stat_local);

	
	for(i=0; i<nFile; i++)	{
		p_MetaData[i].pData = -1;	// Invalid pointer
	}
}

static void Store_More_Files_Locally(void)
{
	int i, FileIdx, nWriteBytes, nDataSize;
	unsigned char *pData;
	int fd_out;
	char szNameOut[256];

	nMoreFilesToStoreReal = 0;

	for(i=0; i<nMoreFilesToStore; i++)	{
		FileIdx = (nMoreFilesToStoreStart + i)%nFile;

//		printf("rank = %d i = %d idx_fs = %d\n", rank, i, p_MetaData[FileIdx].idx_fs);
		if(p_MetaData[FileIdx].idx_fs != rank)	{	// double check here to make sure only transfer non-local files!
			if( (p_stat[FileIdx].st_mode & S_IFMT) != S_IFDIR )	{	// not a directory
//				printf("Rank %d, file %s\n", rank, p_MetaData[FileIdx].szName);
				if( bUsePack && (p_MetaData[FileIdx].size_packed != 0) )	{
					pData = Read_Remote_Packed_File(FileIdx);	// after this call is done, data are ready in pData. 
					nDataSize = p_MetaData[FileIdx].size_packed;
				}
				else	{
					pData = Read_Remote_File(FileIdx);	// after this call is done, data are ready in pData. 
					nDataSize = p_MetaData[FileIdx].size;
				}

				sprintf(szNameOut, "%s/%s", szRoot, p_MetaData[FileIdx].szName);
				fd_out = open(szNameOut, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
				if(fd_out == -1)	{
					printf("Rank %d: Fail to open file %s to write.\nQuit\n", rank, szNameOut);
				}
				else	{
					nWriteBytes = write_all(fd_out, pData, nDataSize);

					if(nWriteBytes != nDataSize)	{
						if(nWriteBytes == OUT_OF_SPACE)	{	// disk is FULL!!!
							ftruncate(fd_out, 0);
							ncx_slab_free(sp_local, pData);	// release memory from the pool
							close(fd_out);
							break;
						}
						else	{
							printf("Error in writing file for %s. nWriteBytes (%d) != nDataSize (%d)\nQuit\n", szNameOut, nWriteBytes, nDataSize);
							exit(1);
						}
					}
					close(fd_out);
				}

				ncx_slab_free(sp_local, pData);	// release memory from the pool

				if( (rank == 0) && (i%20000 == 0) )	printf("Rank 0 just stored %d more files locally.\n", i);

				p_MetaData[FileIdx].idx_fs = rank;	// now this file is stored locally!!!

				nMoreFilesToStoreReal++;
				nFileLocal++;
			}
		}

	}
	printf("Rank = %d: nFileLocal = %d nMoreFilesToStore = %d nMoreFilesToStoreStart= %d\n", rank, nFileLocal, nMoreFilesToStore, nMoreFilesToStoreStart);
}

typedef struct	{
	long int size;
	char szName[256];
}FILEINFO_TOSEND;

static void Broadcast_Shared_Files(void)
{
	int i, ret;
	int fd_bcast=-1, fd_out, ReadBytes=0, WriteBytes;
	char szFileName[256], szOutputName[256], szName[256];
	unsigned char *buff;
	FILEINFO_TOSEND FileInfo;
	struct stat file_stat;

	if(rank == 0)	{
		sprintf(szName, "%s/fs_bcast", szFilePath);
		fd_bcast = open(szName, O_RDONLY);
		if(fd_bcast == -1)	{
			nFile_Bcast = 0;
			printf("No files will be broadcasted.\n");
			MPI_Bcast(&nFile_Bcast, 1, MPI_INT, 0, MPI_COMM_WORLD);
			return;
		}
		else	{
			ReadBytes = read(fd_bcast, &nFile_Bcast, sizeof(int));
			if(ReadBytes != sizeof(int))	{
				printf("Error to read the number of files to broadcast.\nQuit\n");
				exit(1);
			}
		}
	}
	MPI_Bcast(&nFile_Bcast, 1, MPI_INT, 0, MPI_COMM_WORLD);


	if(nFile_Bcast == 0)	{	// no file needs to be broadcasted.
		if(fd_bcast != -1)	{
			close(fd_bcast);
		}
		return;
	}
	
	buff = malloc(MAX_FILE_SIZE);

	for(i=0; i<nFile_Bcast; i++)	{
		if(rank == 0)	{
			read_all(fd_bcast, FileInfo.szName, nLen_File_Name);
			read_all(fd_bcast, &(FileInfo.size), sizeof(long int));
			read_all(fd_bcast, buff, FileInfo.size);

			if( i%20000 == 0 )	printf("Broadcasting file %d\n", i);
		}
		
		MPI_Bcast(&FileInfo, sizeof(FILEINFO_TOSEND), MPI_CHAR, 0, MPI_COMM_WORLD);
		MPI_Bcast(buff, FileInfo.size, MPI_CHAR, 0, MPI_COMM_WORLD);

		sprintf(szOutputName, "%s/%s", szRoot, FileInfo.szName);
		
		fd_out = open(szOutputName, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
		if(fd_out == -1)	{
			ret = stat(szOutputName, &file_stat);
			if( (ret == 0) && ((file_stat.st_mode & S_IFMT) == S_IFDIR) )	{	// Existing dir. Expected behavior.
			}
			else	{
				printf("Rank %d: Fail to open file %s\nQuit\n", rank, szOutputName);
				exit(1);
			}
		}
		else	{
			WriteBytes = write_all(fd_out, buff, FileInfo.size);

			if(WriteBytes != FileInfo.size)	{
				printf("WriteBytes != file_size for file %s. Is disk full?\nQuit\n", szOutputName);
			}
			close(fd_out);
		}
	}

	if( fd_bcast != -1 )	close(fd_bcast);

	free(buff);
}


static void Setup_Hash_Table(void)
{
	int i, value;

	for(i=0; i<nFile; i++) {
		value = DictSearch(p_Hash_File, p_MetaData[i].szName, &elt_list_file, &ht_table_file);
		if(value < 0)	{
			DictInsert(p_Hash_File, p_MetaData[i].szName, i, &elt_list_file, &ht_table_file);
		}
	}
}




#pragma GCC push_options
#pragma GCC optimize ("-O0")


// run when other nodes request files stored in current node
static void *Handle_Remote_Access(void *void_ptr)
{
	int num_read, num_unpacked;
	int file_idx, node_idx;
	MPI_Status status;
	unsigned char *szBuff;
	int counter=0;
	int fd_in;
	char szNameIn[256];

	while(1)	{
		MPI_Recv(&file_idx, 1, MPI_INT, MPI_ANY_SOURCE, (int)MSG_TAG_REQUESTFILE, MPI_COMM_WORLD, &status);
		node_idx = status.MPI_SOURCE;
		if(p_MetaData[file_idx].idx_fs != rank)	{
			printf("File %s (idx %d) can not be found on task %d.\nQuit\n", p_MetaData[file_idx].szName, p_MetaData[file_idx].idx_fs, rank);
			MPI_Finalize();
		}

		if( bUsePack && (p_MetaData[file_idx].size_packed != 0) )	{
			szBuff = ncx_slab_alloc(sp_remote, p_MetaData[file_idx].size_packed);
		}
		else	{
			szBuff = ncx_slab_alloc(sp_remote, p_MetaData[file_idx].size);
		}

		sprintf(szNameIn, "%s/%s", szRoot, p_MetaData[file_idx].szName);
		fd_in = open(szNameIn, O_RDONLY);
		if(fd_in == -1)	{
			printf("Rank %d: Fail to open file %s\nQuit.\n", rank, szNameIn);
			exit(1);
		}

		if( bUsePack && (p_MetaData[file_idx].size_packed != 0) )	{
			num_read = read_all(fd_in, szBuff, p_MetaData[file_idx].size_packed);
			if(num_read != p_MetaData[file_idx].size_packed)	{
				printf("num_read != p_MetaData[file_idx].size_packed. Error in reading file %s on rank %d.\nQuit.\n", p_MetaData[file_idx].szName, rank);
			}
		}
		else	{
			num_read = read_all(fd_in, szBuff, p_MetaData[file_idx].size);
			if(num_read != p_MetaData[file_idx].size)	{
				printf("num_read != p_MetaData[file_idx].size. Error in reading file %s on rank %d.\nQuit.\n", p_MetaData[file_idx].szName, rank);
			}
		}
		
		nByteRead += num_read;
		close(fd_in);
		MPI_Send(szBuff, num_read, MPI_CHAR, node_idx, (int)MSG_TAG_FILECONTENT, MPI_COMM_WORLD);

		ncx_slab_free(sp_remote, szBuff);
		counter++;
	}
}

static unsigned char *Read_Remote_Packed_File(int file_idx)
{
	unsigned char *szBuff;
	MPI_Status status;

	szBuff = ncx_slab_alloc(sp_local, p_MetaData[file_idx].size_packed);

	if(szBuff == NULL)	{
		printf("Failed to allocate memory from sp_local.\nQuit\n");
		exit(1);
	}
	MPI_Send(&file_idx, 1, MPI_INT, p_MetaData[file_idx].idx_fs, (int)MSG_TAG_REQUESTFILE, MPI_COMM_WORLD);
	MPI_Recv(szBuff, p_MetaData[file_idx].size_packed, MPI_CHAR, p_MetaData[file_idx].idx_fs, (int)MSG_TAG_FILECONTENT, MPI_COMM_WORLD, &status);
//	ncx_slab_free(sp_local, szBuff);

	nByteRead += p_MetaData[file_idx].size;

	return szBuff;
}

static unsigned char *Read_Remote_File(int file_idx)
{
	int num_unpacked;
	unsigned char *szBuff, *szBuff_Unpack;
	MPI_Status status;

	if( bUsePack && (p_MetaData[file_idx].size_packed != 0) )	{	// packed file
		szBuff_Unpack = ncx_slab_alloc(sp_local, p_MetaData[file_idx].size + p_MetaData[file_idx].size_packed + 256);	// 256 for buffer 
		szBuff = szBuff_Unpack + p_MetaData[file_idx].size + 128;
	}
	else	{	// non-packed
		szBuff = ncx_slab_alloc(sp_local, p_MetaData[file_idx].size);
	}

	
	if(szBuff == NULL)	{
		printf("Failed to allocate memory from sp_local.\nQuit\n");
		exit(1);
	}
	MPI_Send(&file_idx, 1, MPI_INT, p_MetaData[file_idx].idx_fs, (int)MSG_TAG_REQUESTFILE, MPI_COMM_WORLD);

	if( bUsePack && (p_MetaData[file_idx].size_packed != 0) )	{	// packed file
		MPI_Recv(szBuff, p_MetaData[file_idx].size_packed, MPI_CHAR, p_MetaData[file_idx].idx_fs, (int)MSG_TAG_FILECONTENT, MPI_COMM_WORLD, &status);
		num_unpacked = LZSSE8_Decompress(szBuff, p_MetaData[file_idx].size_packed, szBuff_Unpack, p_MetaData[file_idx].size+128);  // add 128 bytes as extra buffer

		if(num_unpacked != p_MetaData[file_idx].size)	{
			printf("Error in unpacking file %s. num_unpacked = %d org_size = %lld rank = %d\n", p_MetaData[file_idx].szName, num_unpacked, p_MetaData[file_idx].size, rank);
//			exit(1);
		}
		szBuff = szBuff_Unpack;
	}
	else	{
		MPI_Recv(szBuff, p_MetaData[file_idx].size, MPI_CHAR, p_MetaData[file_idx].idx_fs, (int)MSG_TAG_FILECONTENT, MPI_COMM_WORLD, &status);
	}
//	ncx_slab_free(sp_local, szBuff);

	nByteRead += p_MetaData[file_idx].size;

	return szBuff;
}


static void *Handle_Open_Close_File_Queue(void *void_ptr)	// only handl open, close. Once one is called, read the whole file into buffer.
{
	long int job;
	int file_idx, Op_Tag;
	unsigned char *szBuff;
//	int fd_local=-1, num_read;

	*p_front=0;
	*p_back=-1;

	while(1)	{	// loop and check queue forever
		if( *p_back >= *p_front )	{	// queue not empty
			job = dequeue();
			Op_Tag = job >> 32;
			file_idx = job & 0xFFFFFFFF;

			if(Op_Tag == QUEUE_TAG_OPEN)	{
//				if(bDEBUG) printf("Processing queued job: open  file %s\n", p_MetaData[file_idx].szName);
				p_MetaData[file_idx].nOpen++;

				if(p_MetaData[file_idx].pData < 0)	{	// NOT already opened and read in to buffer
					if(p_MetaData[file_idx].idx_fs == rank)	{	// local file
						printf("Something wrong! Local files should be processes in open().\n");
					}
					else	{	// remote file
						p_MetaData[file_idx].pData = (long int)Read_Remote_File(file_idx) - (long int)space_local;
//						p_MetaData[file_idx].nOpen++;
					}
				}
//				p_MetaData[file_idx].nOpen++;
			}
			else if(Op_Tag == QUEUE_TAG_CLOSE)	{
//				if(bDEBUG) printf("Processing queued job: close file %s\n", p_MetaData[file_idx].szName);
				p_MetaData[file_idx].nOpen--;
				if(p_MetaData[file_idx].nOpen <= 0)	{
					if(p_MetaData[file_idx].pData >= 0)	{
//						long int pData_Save=p_MetaData[file_idx].pData;
//						p_MetaData[file_idx].pData = -1;
						long int pData_Save=-1;
						pData_Save = __atomic_exchange_n(&(p_MetaData[file_idx].pData), -1, __ATOMIC_RELAXED);
						ncx_slab_free(sp_local, (void *)(pData_Save + space_local));
//						free((void *)(p_MetaData[file_idx].pData + space_local));
//						p_MetaData[file_idx].pData = -1;
					}
				}
			}
			else if(Op_Tag == QUEUE_TAG_DONE)	{	// nothing is needed since the operations are finished already
			}
			else	{
				printf("Unknow file operation tag %d. Not equal open/close.\nQuit\n", Op_Tag);
				exit(1);
			}
		}
	}

//	close(fd_local);
}

#pragma GCC pop_options

inline void enqueue(void)
{
	while( ( *p_back - *p_front ) >= QUEUE_FULL )	{	// Queue is full. block until queue has enough space
		printf("Queued_Job_List is FULL.\n");
	}

  if (pthread_mutex_lock(p_queue_lock) != 0) {
//  if (pthread_mutex_lock(&mut) != 0) {
    perror("pthread_mutex_lock");
    exit(2);
  }
  (*p_back)++;
  if (pthread_mutex_unlock(p_queue_lock) != 0) {
//  if (pthread_mutex_unlock(&mut) != 0) {
    perror("pthread_mutex_unlock");
    exit(2);
  }
}

inline long int dequeue(void)
{
	long int ret, back_tmp, job_nxt, job_modify;
	int file_idx_cur, Op_Tag_cur;
	int file_idx_nxt, Op_Tag_nxt;

//	if(bDEBUG) printf("dequeue()\n");
	if (pthread_mutex_lock(p_queue_lock) != 0) {
//  if (pthread_mutex_lock(&mut) != 0) {
		perror("pthread_mutex_lock");
		exit(2);
	}
	
	ret = *p_front;
	(*p_front)++;
/*
	ret = Queued_Job_List[ret & QUEUE_FULL];

	file_idx_cur = ret & 0xFFFFFFFF;
	Op_Tag_cur = ret >> 32;
	if(Op_Tag_cur == QUEUE_TAG_CLOSE)	{
		for(back_tmp=(*p_front); back_tmp<=*p_back; back_tmp++)	{	// loop over all expected operations on the same file
			job_nxt = Queued_Job_List[back_tmp & QUEUE_FULL];
			file_idx_nxt = job_nxt & 0xFFFFFFFF;
			Op_Tag_nxt = job_nxt >> 32;
			if( file_idx_nxt == file_idx_cur )	{	// operation for the same file
				if( Op_Tag_nxt == QUEUE_TAG_OPEN )	{	// now this open should be paired with the current CLOSE operation. Set DONE for both. 
					job_modify = ( ((long int)QUEUE_TAG_DONE ) << 32 ) |  ( (long int) file_idx_cur );
					*(Queued_Job_List + (back_tmp & QUEUE_FULL) ) = job_modify;	// operation to be done
					ret = job_modify;	// the current operation
					break;
				}
			}
		}
	}

*/	
	
  if (pthread_mutex_unlock(p_queue_lock) != 0) {
//  if (pthread_mutex_unlock(&mut) != 0) {
	  perror("pthread_mutex_unlock");
	  exit(2);
  }

  ret = Queued_Job_List[ret & QUEUE_FULL];

  return ret;
}

static void Query_DirInfo_File_Size(void)
{
	int ret, fd;
	struct stat file_stat;

	if(rank == 0)	{
		sprintf(szDirInfoFileName, "%s/fs_dirinfo", szFilePath);
		ret = stat(szDirInfoFileName, &file_stat);
		if(ret != 0)	{
			printf("Error to get the file size of %s.\nQuit\n", szDirInfoFileName);
			nDir = 0;
			nDirInfoFileSize = 0;
//			exit(1);
		}
		else	{
			nDirInfoFileSize = file_stat.st_size;

			fd = open(szDirInfoFileName, O_RDONLY);
			if(fd == -1)	{
				printf("Fail to open file: %s\nQuit\n", szDirInfoFileName);
				exit(1);
			}
			read_all(fd, &nDir, sizeof(int));
			close(fd);
		}
	}
	MPI_Bcast(&nDirInfoFileSize, 1, MPI_LONG, 0, MPI_COMM_WORLD);
	MPI_Bcast(&nDir, 1, MPI_INT, 0, MPI_COMM_WORLD);

	if(rank == 0) printf("nDirInfoFileSize = %d  nDir = %d\n", nDirInfoFileSize, nDir);
}

static void Broadcast_DirInfo_and_Setup_Hash_Table(void)
{
	int i, j, ret, fd, offset, value;
	long int nReadBytes;
	
	if(nDir == 0)	{
		return;
	}

	if(rank == 0)	{
		fd = open(szDirInfoFileName, O_RDONLY);
		if(fd == -1)	{
			printf("Fail to open file: %s\nQuit\n", szDirInfoFileName);
			exit(1);
		}
		nReadBytes = read_all(fd, p_DirInfo, nDirInfoFileSize);
		close(fd);

		if(nReadBytes != nDirInfoFileSize)	{
			printf("Error in reading file %s. nReadBytes = %lld nDirInfoFileSize = %lld\nQuit\n", szDirInfoFileName, nReadBytes, nDirInfoFileSize);
		}
	}


	MPI_Bcast(p_DirInfo, nDirInfoFileSize, MPI_CHAR, 0, MPI_COMM_WORLD);

	// now time to set up dir list and hash table
	offset = sizeof(int);
	for(i=0; i<nDir; i++)	{
		strcpy(p_DirRec[i].szDirName, p_DirInfo+offset);
		p_DirRec[i].nEntry = *((int*)( p_DirInfo + offset + nLen_File_Name ));
		p_DirRec[i].addr = offset + nLen_File_Name + sizeof(int);

//		printf("Found %d entries in directory %s\n", p_DirRec[i].nEntry, p_DirRec[i].szDirName);
		offset += (nLen_File_Name + sizeof(int) + p_DirRec[i].nEntry*MAX_DIR_ENTRY_LEN );

//		for(j=0; j<p_DirRec[i].nEntry; j++)	{
//			printf("      %3d %s\n", j, p_DirInfo + p_DirRec[i].addr + j*MAX_DIR_ENTRY_LEN);	// print each entry
//		}

		value = DictSearch(p_Hash_Dir, p_DirRec[i].szDirName, &elt_list_dir, &ht_table_dir);
		if(value < 0)	{
			DictInsert(p_Hash_Dir, p_DirRec[i].szDirName, i, &elt_list_dir, &ht_table_dir);
		}
	}

}


