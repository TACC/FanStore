/*
Written by Lei Huang (huang@tacc.utexas.edu) 
       and Zhao Zhang (zzhang@tacc.utexas.edu) 
       at TACC.
All rights are reserved.
*/

#include <stdio.h>
#include <execinfo.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <malloc.h>
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>
#include <elf.h>
#include <termios.h>

#include <stdarg.h>
//#include <dirent.h>

// read() and fread() behave differently for NONE-seekable file!!! (e.g., /proc/id/map). Look same for seekable file. 

#define __USE_GNU
#define _GNU_SOURCE
#include <dlfcn.h>
#include <link.h>
#include <fcntl.h>

#include "dict.h"
#include "../lzsse8.h"

//#define FAKE_FD	(10000000)	// >= this number will be treated as a fake fd
//#define FAKE_DIR_FD	(20000000)	// >= this number will be treated as a fake fd. Used for dir
#define FAKE_FD			(10000000)	// >= this number will be treated as a fake fd
#define FAKE_DIR_FD	(20000000)	// >= this number will be treated as a fake fd. Used for dir


#define MAX_PATCH	(16)

#define MAX_DIR		(128*1024)
#define MAX_FILE	(2*1024*1024)
#define MAX_DIR_ENTRY_LEN	(64)

#define MAX_FILE_SIZE	(100*1024*1024)

#define QUEUE_SIZE		(4096)
#define QUEUE_FULL		((QUEUE_SIZE) - 1)
#define QUEUE_TAG_OPEN	(0x1)	// tag is stored as the two highest bits.
#define QUEUE_TAG_CLOSE	(0x2)	// tag is stored as the two highest bits.

#define PTHREAD_MUTEXATTR_FLAG_PSHARED (0x80000000)	// int 

#define NHOOK	(5)
#define MIN_MEM_SIZE	(0x1000*2)
#define NULL_RIP_VAR_OFFSET	(0x7FFFFFFF)
#define BOUNCE_CODE_LEN	(25)
#define OFFSET_HOOK_FUNC	(7)

#include "udis86.h"

#define MAX_INSTUMENTS	(24)

long int PACK_TAG=0x4741545F4B434150;	// tag string "PACK_TAG"

static int Max_Bytes_Disassemble=24;

static int input_hook_x(ud_t* u);
static ud_t ud_obj;
static int ud_idx;
static void Setup_Trampoline(void);
static void Init_udis86(void);
static size_t Set_Block_Size(void *pAddr);
static char *szOp=NULL;

int bDbg=0;


#define MAX_INSN_LEN (15)
#define MAX_JMP_LEN (5)
#define MAX_TRAMPOLINE_LEN	((MAX_INSN_LEN) + (MAX_JMP_LEN))

static unsigned char szOp_Bounce[]={0x48,0x89,0x44,0x24,0xf0,0x48,0xb8,0x78,0x56,0x34,0x12,0x78,0x56,0x34,0x12,0x48,0x89,0x44,0x24,0xf8,0x48,0x83,0xec,0x08,0xc3};
// p[7] int type should be replaced by hook function address. 

typedef struct {
	unsigned char trampoline[MAX_TRAMPOLINE_LEN];	// save the orginal function entry code and jump instrument
	unsigned char bounce[BOUNCE_CODE_LEN+3];			// the code can jmp to my hook function. +3 for padding
	void *pOrgFunc;
//	void *pNewFunc;
	int nBytesCopied;	// the number of bytes copied. Needed to remove hook
	int  Offset_RIP_Var;	// the offset of global variable. Relative address has to be corrected when copied into trampoline from original address
}TRAMPOLINE, *PTRAMPOLINE;

//0:  48 89 44 24 f0          mov    QWORD PTR [rsp-0x10],rax
//5:  48 b8 78 56 34 12 78    movabs rax,0x1234567812345678	// Put my new function pointer here!!!!!
//c:  56 34 12
//f:  48 89 44 24 f8          mov    QWORD PTR [rsp-0x8],rax
//14: 48 83 ec 08             sub    rsp,0x8
//18: c3                      ret

TRAMPOLINE *pTrampoline;


static int Inited=0;
static char szRoot[256]="/tmp/fs";
static int uid;
static char szBCastDir[256];	// e.g., "shared/"
static char szScatterDir[256];	// e.g., "/tmp/fs/"
static int nLen_BCastDir=0, nLen_ScatterDir=0;

static Dict p_Hash_File;
static struct elt *elt_list_file;
static int *ht_table_file=NULL;

static Dict p_Hash_Dir;
static struct elt *elt_list_dir;
static int *ht_table_dir=NULL;

//static size_t 	pool_size_local=MAX_FILE_SIZE;
size_t  pool_size_local=256*1024*1024;

static int callback(struct dl_phdr_info *info, size_t size, void *data);
void init_shm(void);
extern void Init_Pointers(void);
void enqueue(int file_idx, int Op);
long int dequeue(void);
int Find_Next_Available_fd(int idx);
void Determine_BCast_Dir(void);


typedef struct
{
  int fd;                     /* File descriptor.  */
  size_t allocation;          /* Space allocated for the block.  */
  size_t size;                /* Total valid data in the block.  */
  size_t offset;              /* Current offset into the block.  */
  off_t filepos;              /* Position of next entry to read.  */
}DIR;

typedef struct
 {
    uint64_t d_ino;
    uint64_t d_off;
    unsigned short int d_reclen;
    unsigned char d_type;
    char d_name[256];           /* We must not include limits.h! */
}dirent;


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
//	char szDirName[176];	// MAX_LEN_FILE
	char szDirName[168];	// MAX_LEN_FILE
}DIRREC;

typedef struct	{
	long int idx;
	unsigned char *pData;	// the pointer to read in buffer
	long int offset;		// current status
	long int size;			// file size
}FILESTATUS, *PFILESTATUS;

#define MAX_OPENED_FILE	(512)
#define MAX_OPENED_FILE_M1	((MAX_OPENED_FILE) - 1)

__thread FILESTATUS FileStatus[MAX_OPENED_FILE];
//FILESTATUS FileStatus[MAX_FILE];
int Next_fd=0;

typedef struct{
	long int base_addr, patch_addr;
	int org_value;
}PATCH_REC;

static int nPatch=0;
PATCH_REC PatchList[MAX_PATCH];

static unsigned long page_size, filter;

static int IsLibpthreadLoaded=0;
static unsigned long int img_mylib_base=0, img_libc_base=0, img_libpthread_base=0;

static void *p_patch=NULL;

//start	to compile list of memory blocks in /proc/pid/maps
#define MAX_NUM_SEG	(1024)
static int nSeg=0;
uint64_t Addr_libc=0;
uint64_t Addr_Min[MAX_NUM_SEG], Addr_Max[MAX_NUM_SEG];
static void Get_Module_Maps(void);
static void* Query_Available_Memory_BLock(void *);
//end	to compile list of memory blocks in /proc/pid/maps

#define NHOOK_IN_PTHREAD	(4)

static long int func_addr[NHOOK], func_addr_in_lib_pthread[NHOOK_IN_PTHREAD];
static long int func_len[NHOOK], func_len_in_lib_pthread[NHOOK];
static long int org_func_addr[NHOOK], org_func_len[NHOOK];
static long int org_code_saved_lib_pthread[NHOOK_IN_PTHREAD];

#define MAX_LEN_FUNC_NAME	(36)

//static char szFunc_List[NHOOK][36]={"my_open", "my_close", "my_lxstat", "my_xstat", "my_fxstat", "my_read", "my_lseek", "my_write", "my_opendir", "my_closedir", "my_readdir"};
//static char szOrgFunc_List[NHOOK][36]={"open", "close", "__lxstat", "__xstat", "__fxstat", "read", "lseek", "write", "opendir", "closedir", "readdir"};

//static char szFunc_List[NHOOK][MAX_LEN_FUNC_NAME]={"my_open", "my_close_nocancel", "my_read", "my_lseek", "my_write", "my_lxstat", "my_xstat", "my_fxstat", "my_opendir", "my_closedir", "my_readdir", "my_fflush", "my_feof", "my_ferror"};
//static char szOrgFunc_List[NHOOK][MAX_LEN_FUNC_NAME]={"open", "__close_nocancel", "read", "lseek", "write", "__lxstat", "__xstat", "__fxstat", "opendir", "closedir", "readdir", "fflush", "feof", "ferror"};

static char szFunc_List[NHOOK][MAX_LEN_FUNC_NAME]={"my_open", "my_close_nocancel", "my_read", "my_lseek", "my_fxstat"};
static char szOrgFunc_List[NHOOK][MAX_LEN_FUNC_NAME]={"open64", "__close_nocancel", "__read", "lseek64", "__fxstat"};


typedef int (*org_open)(const char *pathname, int oflags,...);
static org_open real_open=NULL;

typedef int (*org_close)(int fd);
static org_close real_close=NULL;

typedef int (*org_close_nocancel)(int fd);
static org_close_nocancel real_close_nocancel=NULL;
int my_close_nocancel(int fd);

typedef ssize_t (*org_read)(int fd, void *buf, size_t count);
static org_read real_read=NULL;

typedef int (*org_lxstat)(int __ver, const char *__filename, struct stat *__stat_buf);
static org_lxstat real_lxstat=NULL;

typedef int (*org_xstat)(int __ver, const char *__filename, struct stat *__stat_buf);
static org_xstat real_xstat=NULL;

typedef int (*org_fxstat)(int vers, int fd, struct stat *buf);
static org_fxstat real_fxstat=NULL;

typedef off_t (*org_lseek)(int fd, off_t offset, int whence);
static org_lseek real_lseek=NULL;

typedef int (*org_write)(int fd, const void *buf, size_t count);
static org_write real_write=NULL;

typedef DIR * (*org_opendir)(const char *__name);
static org_opendir real_opendir=NULL;

typedef int (*org_closedir)(DIR *__dirp);
static org_closedir real_closedir=NULL;

typedef dirent* (*org_readdir)(DIR *__dirp);
static org_readdir real_readdir=NULL;

typedef int (*org_ferror)(FILE *stream);
static org_ferror real_ferror=NULL;

typedef size_t (*org_fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream);
static org_fwrite real_fwrite=NULL;

typedef size_t (*org_fread)(void *ptr, size_t size, size_t nmemb, FILE *stream);
static org_fread real_fread=NULL;


typedef int (*org_isatty)(int fd);
static org_isatty real_isatty=NULL;

typedef int (*org_tcgetattr)(int fd, struct termios *termios_p);
static org_tcgetattr real_tcgetattr=NULL;


static int Get_Position_of_Next_Line(char szBuff[], int iPos, int nBuffSize);
static void Query_Original_Func_Address(void);

static char szPathLibc[128]="";
static char szPathLibpthread[128];
static char szPathMyLib[256];  // the name of myself

static void *p_shm_ht=NULL;	// ptr to shared memory
static int shm_ht_fd;	//
static char szNameHTShm[]="fs_ht_shm";
static int nSize_Shared_Memory_HT=0;
long int *Queued_Job_List;
pthread_mutex_t *p_queue_lock;
long int *p_front=NULL, *p_back=NULL;

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
static unsigned char *p_File_Buff;

FILEREC *p_MetaData;	// the array of meta data. Same on all nodes.
struct stat *p_stat;	// the array of stat info of all files

DIRREC *p_DirRec;
unsigned char *p_DirInfo;

static int nDir=0;

static void *p_shm_file_buff=NULL;	// ptr to shared memory
static int shm_file_buff_fd;	//
static char szNameFileBuffShm[]="fs_buff_shm";
static int nSize_Shared_Memory_File_Buff=0;

long int back_save=-1;

unsigned long int rdtscp(void)
{
    unsigned long int rax, rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=d" (rdx) : : );
    return (rdx << 32) + rax;
}


int close(int fd)
{
	int fd_idx;
	int ret;

	if(bDbg) printf("DBG> close(%d)\n", fd);

	if(real_close==NULL)	{
		real_close = (org_close)dlsym(RTLD_NEXT, "close");
	}

	if(fd >= FAKE_FD)	{	// already in buffer
		fd_idx = fd - FAKE_FD;
//		printf("To push close into queue.\n");
		if(p_MetaData[FileStatus[fd_idx].idx].idx_fs == *p_myrank)	{	// local

			free(FileStatus[fd_idx].pData); // !!!!!!!!!!!!!!!!!!!!!!!!!
			
			FileStatus[fd_idx].pData = 0;	// recycle this fd
		}
		else	{	// remote file
			if( (FileStatus[fd_idx].pData) && (fd_idx<MAX_OPENED_FILE) ) enqueue(FileStatus[fd_idx].idx, QUEUE_TAG_CLOSE);
			FileStatus[fd_idx].pData = 0;	// recycle this fd
		}
		return 0;	// success!!
	}
	else	{
		ret = real_close(fd);
		if(bDbg) printf("DBG> In my close(). fd = %d  ret = %d errno = %d\n", fd, ret, errno);
		return ret;
	}
}
extern int __close(int fd) __attribute__ ( (alias ("close")) );

int my_close_nocancel(int fd)
{
	int fd_idx;
	int ret;

//	if(bDbg) printf("DBG> close_nocancel(%d)\n", fd);

	if(fd >= FAKE_FD)	{	// already in buffer
		fd_idx = fd - FAKE_FD;
//		printf("To push close into queue.\n");

		if(p_MetaData[FileStatus[fd_idx].idx].idx_fs == *p_myrank)	{	// local
			free(FileStatus[fd_idx].pData); // !!!!!!!!!!!!!!!!!!!!!!!!!
			FileStatus[fd_idx].pData = 0;	// recycle this fd
		}
		else	{	// remote file
			if( (FileStatus[fd_idx].pData) && (fd_idx<MAX_OPENED_FILE) ) enqueue(FileStatus[fd_idx].idx, QUEUE_TAG_CLOSE);
			FileStatus[fd_idx].pData = 0;	// recycle this fd
		}


//		if( (FileStatus[fd_idx].pData) && (fd_idx<MAX_OPENED_FILE) )  enqueue(FileStatus[fd_idx].idx, QUEUE_TAG_CLOSE);
//		FileStatus[fd_idx].pData = 0;	// recycle this fd
		return 0;	// success!!
	}
	else	{
		ret = real_close_nocancel(fd);
//		if(bDbg) printf("DBG> In my close(). fd = %d  ret = %d errno = %d\n", fd, ret, errno);
		return ret;
	}
}

/*
int my_IO_file_close(int fd)
{
	int fd_idx;
/*
//	int ret;
//	char szBuff[256];

//	sprintf(szBuff, "DBG> IO_file_close(%d)\n", fd);
//	write(1, szBuff, strlen(szBuff));

	if(fd >= FAKE_FD)	{	// already in buffer
		fd_idx = fd - FAKE_FD;
//		printf("To push close into queue.\n");
		enqueue(FileStatus[fd_idx].idx, QUEUE_TAG_CLOSE);
		FileStatus[fd_idx].pData = 0;	// recycle this fd
		return 0;	// success!!
	}
	else	{
*/
//		ret = real_IO_file_close(fd);
//		sprintf(szBuff, "DBG> In my IO_file_close(). fd = %d  ret = %d errno = %d\n", fd, ret, errno);
//		write(1, szBuff, strlen(szBuff));
//		return real_IO_file_close(fd);
//		return real_close(fd);
//	}
//}


static ssize_t read_all(int fd, void *buf, size_t count)
{
	ssize_t ret, nBytes=0;

	while (count != 0 && (ret = real_read(fd, buf, count)) != 0) {
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


ssize_t my_read(int fd, void *buf, size_t count)
{
	int fd_idx;
	long int BytesLeft;
	ssize_t ret;

//	if(bDbg) printf("DBG> read(%d, , %d)\n", fd, count);

//	if(real_read==NULL)	{
//		real_read = (org_read)dlsym(RTLD_NEXT, "read");
//	}

	if(fd >= FAKE_FD)	{	// already in buffer
		fd_idx = fd - FAKE_FD;
		BytesLeft = FileStatus[fd_idx].size - FileStatus[fd_idx].offset;

		if( (FileStatus[fd_idx].pData[0] == 0) && (FileStatus[fd_idx].pData[1] == 8) ) {
			printf("BAD file content found. back_save = %lld nOpen = %d\n", back_save, p_MetaData[FileStatus[fd_idx].idx].nOpen);
		}

//                if(count==4194304) printf("DBG> Read 4194304 bytes from file %s, Return %zu. Offset = %lld ByteLeft = %lld\n", p_MetaData[FileStatus[fd_idx].idx].szName, count, FileStatus[fd_idx].offset, BytesLeft);

		if(BytesLeft > count)	{
			memcpy(buf, FileStatus[fd_idx].pData+FileStatus[fd_idx].offset, count);
			FileStatus[fd_idx].offset += count;
//			if(bDbg) printf("DBG> In my read(). Ret = %lld\n", count);
//			printf("DBG> In my read(). Ret = %lld\n", count);
//			if(count==4194304) printf("DBG> Read 4194304 bytes from file %s, Return %zu. Offset = %lld\n", p_MetaData[FileStatus[fd_idx].idx].szName, count, FileStatus[fd_idx].offset);
			return count;
		}
		else if(BytesLeft > 0)	{
			memcpy(buf, FileStatus[fd_idx].pData+FileStatus[fd_idx].offset, BytesLeft);
			FileStatus[fd_idx].offset += BytesLeft;
//			if(bDbg) printf("DBG> In my read(). Ret = %lld\n", BytesLeft);
//			printf("DBG> In my read(). Ret = %lld\n", BytesLeft);
//                        if(count==4194304) printf("DBG> Read 4194304 bytes from file %s, Return %zu. Offset = %lld\n", p_MetaData[FileStatus[fd_idx].idx].szName, BytesLeft, FileStatus[fd_idx].offset);
			return BytesLeft;
		}
		else if(BytesLeft <= 0)	{	// reached the end of this file
//			if(bDbg) printf("DBG> In my read(). Ret = 0\n");
//			printf("DBG> In my read(). Ret = 0\n");
//                        if(count==4194304) printf("DBG> Read 4194304 bytes from file %s, Return %zu. Offset = %lld\n", p_MetaData[FileStatus[fd_idx].idx].szName, 0, FileStatus[fd_idx].offset);			
			return 0;
		}
	}
	else	{
		ret = real_read(fd, buf, count);
//		if(bDbg) printf("DBG> In my read(). Ret = %lld. errno = %d\n", ret, errno);
//		printf("DBG> In my read(). Ret = %lld. errno = %d\n", ret, errno);
		return ret;
	}

}
//extern ssize_t __read(int fd, void *buf, size_t count) __attribute__ ( (alias ("read")) );

#pragma GCC push_options
#pragma GCC optimize ("-O0")

#define LONG_LOOP	(500000000)
#define LONGER_LOOP	((LONG_LOOP)*20)
//int open(const char *pathname, int oflags, ...)

//static struct timespec tim1, tim2;

//char fbuff[1024*1024*64];
int my_open(const char *pathname, int oflags, ...)
{
	int mode = 0, two_args=1, i, Open_Listed_File=0, Open_Remote_File=0;
	int ret, file_idx, fd, nBytesRead, nBytesUnpacked;
	char szName[256], *pSubStr;
	long int counter, count_max, count_max_max;
// #define LONG_LOOP       (500000000)
// #define LONGER_LOOP     ((LONG_LOOP)*20)

        count_max = 5000000000;
	count_max_max = count_max * 200;
	back_save = -1;

	if (oflags & O_CREAT)	{
		va_list arg;
		va_start (arg, oflags);
		mode = va_arg (arg, int);
		va_end (arg);
		two_args=0;
	}

//	if(real_open==NULL)	{
//		real_open = (org_open)dlsym(RTLD_NEXT, "open");
//	}
	strcpy(szName, pathname);
//	printf("Tag 0. %s\n", pathname);

	if(Inited == 1)	{
		if(szBCastDir[0])	{	// a valid bcast dir string
			pSubStr = strstr(pathname, szBCastDir);	// containing the tag of bcast. The files exist on all nodes. 
			if(pSubStr)	{
				if(pathname[0] == '/')	{	// no need to prepend path
//					strcpy(szName, pathname);
				}
				else	{
					sprintf(szName, "%s/%s", szRoot, pSubStr+nLen_BCastDir);	// compose the file name
				}

				Open_Listed_File = 1;
//				if(two_args)    {
//					ret = real_open(szName, oflags);
//				}
//				else    {
//					ret = real_open(szName, oflags, mode);
//				}
//				return ret;
			}
		}

		if( szScatterDir[0] && (! Open_Listed_File) )	{	// a valid scatter dir string
			pSubStr = strstr(pathname, szScatterDir);
			if(pSubStr)	{
				file_idx = DictSearch(p_Hash_File, pSubStr+nLen_ScatterDir, &elt_list_file, &ht_table_file);
				if(file_idx >= 0)	{	// Found in the hash table
					Open_Listed_File = 1;
					if(p_MetaData[file_idx].idx_fs == *p_myrank)	{	// local file. Directly open works!
						if(pathname[0] == '/')	{	// no need to prepend path
//							strcpy(szName, pathname);
						}
						else	{
							sprintf(szName, "%s/%s", szRoot, pSubStr+nLen_ScatterDir);	// compose the file name
						}
					}
					else	{	// Request from other nodes through serialized IO queue
						Open_Remote_File = 1;
					}
				}
			}
		}

/*
		if(Run_Org_Open)	{	// another chance to double check
			file_idx = DictSearch(p_Hash_File, pathname, &elt_list_file, &ht_table_file);
//			file_idx = DictSearch(p_Hash_File, pathname);
			if(file_idx >= 0)	{	// Found in the hash table
				Run_Org_Open = 0;
			}
		}
//		printf("%s: Run_Org_Open = %d\n", pathname, Run_Org_Open);
*/
	}

	if(Open_Listed_File)	{
		if(Open_Remote_File)	{	// request file form other nodes
			enqueue(file_idx, QUEUE_TAG_OPEN);
			back_save = *p_back;

//			tim1.tv_sec = 0;
//			tim1.tv_nsec = 1000;
//			for(counter=0; counter<5000000; counter++)  {
//				nanosleep(&tim1, &tim2);
			for(counter=0; counter<count_max; counter++)  {	// The max waiting time is on the order of second.
                		if(p_MetaData[file_idx].pData >= 0)       {
					break;
                		}
			}
			if(p_MetaData[file_idx].pData < 0)	{	// data is still NOT ready yet. Send another request...
				printf("DBG> Time out for requesting from server %d for file %s to read. Try again.\n", p_MetaData[file_idx].idx_fs, pathname);
//				enqueue(file_idx, QUEUE_TAG_OPEN);
//				for(counter=0; counter<80000000; counter++)  {     // another round
//					nanosleep(&tim1, &tim2);
				for(counter=0; counter<count_max_max; counter++)  {	// another round
					if(p_MetaData[file_idx].pData > 0)       {
						break;
					}
				}
				if(p_MetaData[file_idx].pData < 0)	{	// still failed to get file ready after longer waiting time. Quit. 
					printf("DBG> Time out for retrying requesting file %s to read. Quit.\n", pathname);
					exit(1);
				}
			}
			
			Next_fd = Find_Next_Available_fd(Next_fd);
			FileStatus[Next_fd].idx = file_idx;
			FileStatus[Next_fd].offset = 0;	// the beginning of the file
			FileStatus[Next_fd].pData = (unsigned char *)(p_File_Buff + p_MetaData[file_idx].pData);	// Unpacked already
			FileStatus[Next_fd].size = p_MetaData[file_idx].size;
			
			ret = Next_fd + FAKE_FD;

//			long int ts;
//			char *pByte;
//			ts = rdtscp();
//			pByte = (char*)FileStatus[Next_fd].pData;
//			printf("DBG> Open(), File %d, pData = %llx, [%x %x] time = %lld\n", file_idx, p_MetaData[file_idx].pData, pByte[0], pByte[1], ts);
		}
		else	{	// local file. Maybe packed
			if( *p_bUsePack && (p_MetaData[file_idx].size_packed != 0) )	{	// packed file. Need to read in and unpack
				Next_fd = Find_Next_Available_fd(Next_fd);

				FileStatus[Next_fd].idx = file_idx;
				FileStatus[Next_fd].offset = 0;	// the beginning of the file

//				FileStatus[Next_fd].pData = fbuff;

				FileStatus[Next_fd].pData = malloc(p_MetaData[file_idx].size + p_MetaData[file_idx].size_packed + 256);	// with 256 bytes as buffer
				FileStatus[Next_fd].size = p_MetaData[file_idx].size;
//				printf("File %s size = %lld size_packed = %lld\n", p_MetaData[file_idx].szName, p_MetaData[file_idx].size, p_MetaData[file_idx].size_packed);

				// read real file, unpack and close file. Return a fake fd
				fd = real_open(szName, oflags);
				if(fd == -1)	{
					printf("Failed to open packed listed file: %s\nQuit\n", szName);
					perror("open");
					exit(1);
				}
				nBytesRead = read_all(fd, FileStatus[Next_fd].pData + p_MetaData[file_idx].size + 128, p_MetaData[file_idx].size_packed);	// read in packed file
				if(nBytesRead != p_MetaData[file_idx].size_packed)	{
					printf("nBytesRead (%d) != p_MetaData[file_idx].size_packed (%lld) for %s\n", nBytesRead, p_MetaData[file_idx].size_packed, p_MetaData[file_idx].szName);
					exit(1);
				}
				real_close(fd);
				nBytesUnpacked = LZSSE8_Decompress(FileStatus[Next_fd].pData + p_MetaData[file_idx].size + 128, p_MetaData[file_idx].size_packed, FileStatus[Next_fd].pData, p_MetaData[file_idx].size + 128);	// add 128 bytes as extra buffer
				if(nBytesUnpacked != p_MetaData[file_idx].size)	{
					printf("nBytesUnpacked (%d) != p_MetaData[file_idx].size (%lld) for %s\n", nBytesUnpacked, p_MetaData[file_idx].size, p_MetaData[file_idx].szName);
				}
				ret = Next_fd + FAKE_FD;
			}
			else	{	// like original call
				if(two_args)    {
					ret = real_open(szName, oflags);
				}
				else    {
					ret = real_open(szName, oflags, mode);
				}
			}
		}

//		printf("DBG> Opening %s. Ret code = %d\n", pathname, ret);
	}
	else	{	// orginal open() for non-listed file
		if(two_args)    {
			ret = real_open(szName, oflags);
		}
		else    {
			ret = real_open(szName, oflags, mode);
		}
	}


//	if(bDbg) printf("DBG> Opening %s. Ret code = %d\n", pathname, ret);

	return ret;
}
//extern int __open(const char *pathname, int oflags, ...) __attribute__ ( (alias ("open")) );
//extern int open64(const char *pathname, int oflags, ...) __attribute__ ( (alias ("open")) );
//extern int __open64(const char *pathname, int oflags, ...) __attribute__ ( (alias ("open")) );


#pragma GCC pop_options


int lxstat(int __ver, const char *__filename, struct stat *__stat_buf)
{
	int ret, file_idx, Run_Org_Stat=1;
	char szName[256], *pSubStr;
	
//	if(bDbg) printf("DBG> In my lxstat() for %s.\n", __filename);

	if(real_lxstat==NULL)	{
		real_lxstat = (org_lxstat)dlsym(RTLD_NEXT, "__lxstat");
	}

	if(Inited == 0) {       // init() not finished yet
		ret = real_lxstat(__ver, __filename, __stat_buf);
		return ret;
	}	
	else	{
		if(szBCastDir[0])	{	// a valid bcast dir string
			pSubStr = strstr(__filename, szBCastDir);	// containing the tag of bcast. The files exist on all nodes. 
			if(pSubStr)	{
				if(__filename[0] == '/')	{	// no need to prepend path
					strcpy(szName, __filename);
				}
				else	{
					sprintf(szName, "%s/%s", szRoot, pSubStr+nLen_BCastDir);	// compose the file name
				}

				ret = real_lxstat(__ver, szName, __stat_buf);
				return ret;
			}
		}

		if(szScatterDir[0])	{	// a valid scatter dir string
			pSubStr = strstr(__filename, szScatterDir);	// containing the tag of bcast. The files exist on all nodes. 
//			printf("In lxstat(). filename = %s, szScatterDir = %s, pSubStr = %p, *pSubStr = %s\n", __filename, szScatterDir, pSubStr, pSubStr+nLen_ScatterDir);
			if(pSubStr)	{
				file_idx = DictSearch(p_Hash_File, pSubStr+nLen_ScatterDir, &elt_list_file, &ht_table_file);
//				file_idx = DictSearch(p_Hash_File, pSubStr);
				if(file_idx >= 0)	{	// Found in the hash table
//					printf("Found %s in hash table. idx = %d\n", pSubStr+nLen_ScatterDir, file_idx);
					Run_Org_Stat = 0;
				}
			}
		}
/*
		if(Run_Org_Stat)	{	// another chance to double check
			file_idx = DictSearch(p_Hash_File, __filename, &elt_list_file, &ht_table_file);
//			file_idx = DictSearch(p_Hash_File, __filename);
			if(file_idx >= 0)	{	// Found in the hash table
				Run_Org_Stat = 0;
			}
		}
*/
	}
	
	if(Run_Org_Stat == 1)	{
//		printf("Calling original lxstat.\n");
		ret = real_lxstat(__ver, __filename, __stat_buf);
	}
	else	{
//		printf("Filling existing stat info.\n");
		memcpy(__stat_buf, &(p_stat[file_idx]), sizeof(struct stat));
		ret = 0;
	}
	
	return ret;
}
extern int _lxstat(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );
extern int __lxstat(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );
extern int ___lxstat(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );
extern int __lxstat64(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("lxstat")) );


int xstat(int __ver, const char *__filename, struct stat *__stat_buf)
{
	int ret, file_idx, Run_Org_Stat=1;
	char szName[256], *pSubStr, szBuff[256];

//	sprintf(szBuff, "DBG> In my xstat() for %s.\n", __filename);
//	write(1, szBuff, strlen(szBuff));

	if(real_xstat==NULL)	{
		real_xstat = (org_xstat)dlsym(RTLD_NEXT, "__xstat");
	}
	
	if(Inited == 0) {       // init() not finished yet
		ret = real_xstat(__ver, __filename, __stat_buf);
		return ret;
	}	
	else	{
		if(szBCastDir[0])	{	// a valid bcast dir string
			pSubStr = strstr(__filename, szBCastDir);	// containing the tag of bcast. The files exist on all nodes. 
			if(pSubStr)	{
				if(__filename[0] == '/')	{	// no need to prepend path
					strcpy(szName, __filename);
				}
				else	{
					sprintf(szName, "%s/%s", szRoot, pSubStr+nLen_BCastDir);	// compose the file name
				}
				
				ret = real_xstat(__ver, szName, __stat_buf);
				return ret;
			}
		}

		if(szScatterDir[0])	{	// a valid scatter dir string
			pSubStr = strstr(__filename, szScatterDir);	// containing the tag of bcast. The files exist on all nodes. 
			if(pSubStr)	{
				file_idx = DictSearch(p_Hash_File, pSubStr+nLen_ScatterDir, &elt_list_file, &ht_table_file);
				if(file_idx >= 0)	{	// Found in the hash table
					Run_Org_Stat = 0;
				}
			}
		}
/*
		if(Run_Org_Stat)	{	// another chance to double check
			file_idx = DictSearch(p_Hash_File, __filename, &elt_list_file, &ht_table_file);
			if(file_idx >= 0)	{	// Found in the hash table
				Run_Org_Stat = 0;
			}
		}
*/
	}
	
	if(Run_Org_Stat == 1)	{
		ret = real_xstat(__ver, __filename, __stat_buf);
	}
	else	{
		memcpy(__stat_buf, &(p_stat[file_idx]), sizeof(struct stat));
		ret = 0;
	}
	
	return ret;
}
extern int __xstat(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("xstat")) );
extern int __xstat64(int __ver, const char *__filename, struct stat *__stat_buf) __attribute__ ( (alias ("xstat")) );

int my_fxstat(int vers, int fd, struct stat *buf)
{
	int fd_idx;
	char szBuff[256];

//	sprintf(szBuff, "DBG> In my fxstat() for %d.\n", fd);
//	write(1, szBuff, strlen(szBuff));

	if(fd >= FAKE_FD)	{	// already in buffer
		fd_idx = fd - FAKE_FD;
		memcpy(buf, &(p_stat[FileStatus[fd_idx].idx]), sizeof(struct stat));
		return 0;
	}
	else	{
		return real_fxstat(vers, fd, buf);
	}
}


off_t my_lseek(int fd, off_t offset, int whence)
{
	int fd_idx;
	char szBuff[256];

//	sprintf(szBuff, "DBG> In my lseek() for %d.\n", fd);
//	if(bDbg) write(1, szBuff, strlen(szBuff));

//	if(real_lseek==NULL)	{
//		real_lseek = (org_lseek)dlsym(RTLD_NEXT, "lseek64");
//	}

	if(fd >= FAKE_FD)	{
		fd_idx = fd - FAKE_FD;
		if(whence == SEEK_SET)	{	// beginning
			FileStatus[fd_idx].offset = offset;
		}
		else if(whence == SEEK_CUR)	{	// current position
			FileStatus[fd_idx].offset += offset;
		}
		else if(whence == SEEK_END)	{	// end
			FileStatus[fd_idx].offset = FileStatus[fd_idx].size + offset;
		}

		return (off_t)(FileStatus[fd_idx].offset);
	}
	else	{
		return real_lseek(fd, offset, whence);
	}
}
/*
extern off_t llseek(int fd, off_t offset, int whence) __attribute__ ( (alias ("lseek")) );
extern off_t __llseek(int fd, off_t offset, int whence) __attribute__ ( (alias ("lseek")) );
extern off_t __lseek(int fd, off_t offset, int whence) __attribute__ ( (alias ("lseek")) );
extern off_t lseek64(int fd, off_t offset, int whence) __attribute__ ( (alias ("lseek")) );
extern off_t __lseek64(int fd, off_t offset, int whence) __attribute__ ( (alias ("lseek")) );
*/

ssize_t write(int fd, const void *buf, size_t count)
{
	int fd_idx;

	if(real_write==NULL)	{
		real_write = (org_write)dlsym(RTLD_NEXT, "write");
	}
	
	if(fd >= FAKE_FD)	{
		fd_idx = fd - FAKE_FD;
		printf("Trying to write file: %s. Writing is NOT supported yet.\nQuit\n", p_MetaData[FileStatus[fd_idx].idx].szName);
		exit(1);
	}

	return real_write(fd, buf, count);
}
extern ssize_t __write(int fd, const void *buf, size_t count) __attribute__ ( (alias ("write")) );

DIR *opendir(const char *__name)
{
	int ret, dir_idx, Run_Org_Opendir=1;
	char szName[256], *pSubStr;
	DIR *p_Dir;

//	if(bDbg) printf("DBG> In my opendir() for %s.\n", __name);

	if(real_opendir==NULL)	{
		real_opendir = (org_opendir)dlsym(RTLD_NEXT, "opendir");
	}
	
	if(Inited == 0) {       // init() not finished yet
		return real_opendir(__name);
	}	
	else	{
		if(szBCastDir[0])	{	// a valid bcast dir string
			pSubStr = strstr(__name, szBCastDir);	// containing the tag of bcast. The files exist on all nodes. 
			if(pSubStr)	{
				if(__name[0] == '/')	{	// no need to prepend path
					strcpy(szName, __name);
				}
				else	{
					sprintf(szName, "%s/%s", szRoot, pSubStr+nLen_BCastDir);	// compose the file name
				}
				
				return real_opendir(szName);
			}
		}

		if(szScatterDir[0])	{	// a valid scatter dir string
			pSubStr = strstr(__name, szScatterDir);	// containing the tag of bcast. The files exist on all nodes. 
			if(pSubStr)	{
      	int nStrLen, idx;
        strcpy(szName, __name);
        nStrLen = strlen(szName);
        while( (nStrLen > 1) && (szName[nStrLen-1] == '/') ) {  // end with '/' which should be removed.
        	szName[nStrLen-1] = 0; // remove '/'
          nStrLen--;
        }

        dir_idx = DictSearch(p_Hash_Dir, szName + (pSubStr - __name) + nLen_ScatterDir, &elt_list_dir, &ht_table_dir);
				if(dir_idx >= 0)	{	// Found in the hash table
//					printf("Found %s in hash table. idx = %d\n", pSubStr+nLen_ScatterDir, file_idx);
					Run_Org_Opendir = 0;
				}
			}
		}

		if(Run_Org_Opendir)	{	// another chance to double check
			dir_idx = DictSearch(p_Hash_Dir, __name, &elt_list_dir, &ht_table_dir);
			if(dir_idx >= 0)	{	// Found in the hash table
				Run_Org_Opendir = 0;
			}
		}
	}
	
	if(Run_Org_Opendir == 1)	{
//		printf("Calling original opendir.\n");
		return real_opendir(__name);
	}
	else	{
//		printf("Calling lei's opendir.\n");
		p_Dir = (DIR *)malloc(sizeof(DIR));
		p_Dir->fd = FAKE_DIR_FD + dir_idx;
		p_Dir->size = p_DirRec[dir_idx].nEntry;
		p_Dir->offset = 0;	// starting from the first entry

		return p_Dir;
	}
}
extern DIR * __opendir(const char *__name) __attribute__ ( (alias ("opendir")) );

int closedir(DIR *__dirp)
{
//	if(bDbg) printf("DBG> In my closedir().\n");

	if(real_closedir==NULL)	{
		real_closedir = (org_closedir)dlsym(RTLD_NEXT, "closedir");
	}

	if( Inited == 0 ) {       // init() not finished yet
		return real_closedir(__dirp);
	}
	
	if(__dirp == NULL)	{
		printf("__dirp == NULL in closedir().\nQuit\n");
		return (-1);
	}
	else	{
		if(__dirp->fd >= FAKE_DIR_FD)	{	// dir fd
			__dirp->fd = -1;
			free(__dirp);
			return 0;
		}
		else	{
			return real_closedir(__dirp);
		}
	}
}

extern int __closedir(DIR *__dirp) __attribute__ ( (alias ("closedir")) );


__thread dirent one_dirent;

dirent *readdir(DIR *__dirp)
{
	int dir_idx;

//	if(bDbg) printf("DBG> In my readdir().\n");

	if(real_readdir==NULL)	{
		real_readdir = (org_readdir)dlsym(RTLD_NEXT, "readdir");
	}

	if( Inited == 0 ) {       // init() not finished yet
		return real_readdir(__dirp);
	}
	
	if(__dirp == NULL)	{
		printf("__dirp == NULL in readdir().\nQuit\n");
		return NULL;
	}
	else	{
		if(__dirp->fd >= FAKE_DIR_FD)	{	// dir fd
			dir_idx = __dirp->fd - FAKE_DIR_FD;
			if(__dirp->offset == 0)	{
				strcpy(one_dirent.d_name, ".");
				__dirp->offset ++;
				return &one_dirent;
			}
			else if(__dirp->offset == 1)	{
				strcpy(one_dirent.d_name, "..");
				__dirp->offset ++;
				return &one_dirent;
			}
			else if(__dirp->offset < (__dirp->size + 2) )	{
				strcpy(one_dirent.d_name, p_DirInfo + p_DirRec[dir_idx].addr + (__dirp->offset - 2)*MAX_DIR_ENTRY_LEN);
				__dirp->offset ++;
				return &one_dirent;
			}
			else	{
				return NULL;
			}
		}
		else	{
			return real_readdir(__dirp);
		}
	}
}
extern struct dirent * __readdir(DIR *__dirp) __attribute__ ( (alias ("readdir")) );
extern struct dirent * readdir64(DIR *__dirp) __attribute__ ( (alias ("readdir")) );

int isatty(int fd)
{
	if(real_isatty==NULL)	{
		real_isatty = (org_isatty)dlsym(RTLD_NEXT, "isatty");
	}

	if(fd >= FAKE_FD)	{
		return 0;	// non-terminal
	}
	else	{
		return real_isatty(fd);
	}
}
extern int __isatty(int fd) __attribute__ ( (alias ("isatty")) );


int tcgetattr(int fd, struct termios *termios_p)
{
	if(real_tcgetattr==NULL)	{
		real_tcgetattr = (org_tcgetattr)dlsym(RTLD_NEXT, "tcgetattr");
	}

	if(fd >= FAKE_FD)	{
		if(termios_p)	{	// filling termios structure
			termios_p->c_iflag = 0;
			termios_p->c_oflag = 0;
			termios_p->c_cflag = 2863403975;
			termios_p->c_lflag = 10922;
			termios_p->c_line = 1;
			termios_p->c_ispeed = 0;
			termios_p->c_ospeed = 4195840;
			termios_p->c_cc[0] = 0;
		}
		return (-1);
	}
	else	{
		return real_tcgetattr(fd, termios_p);
	}
}
extern int __tcgetattr(int fd) __attribute__ ( (alias ("tcgetattr")) );


/*
int ferror(FILE *stream)
{
	int ret;
	char szBuff[256];

	if(real_ferror==NULL)	{
		real_ferror = (org_ferror)dlsym(RTLD_NEXT, "ferror");
	}

	if(stream->_fileno >= FAKE_FD)	{
		return 0;
	}

	ret = real_ferror(stream);

	sprintf(szBuff, "DBG> In my ferror(). ret = %d flag = %d fileno = %d\n", ret, stream->_flags, stream->_fileno);
	if(bDbg) write(1, szBuff, strlen(szBuff));

	return ret;
}
extern int _IO_ferror(FILE *stream) __attribute__ ( (alias ("ferror")) );
*/


/*
size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
	char szBuff[256];

	sprintf(szBuff, "DBG> In my fwrite(%d, %lld, %lld).\n", stream->_fileno, (long int)size, (long int)nmemb);
	if(bDbg) write(1, szBuff, strlen(szBuff));

	if(real_fwrite==NULL)	{
		real_fwrite = (org_fwrite)dlsym(RTLD_NEXT, "fwrite");
	}

	return real_fwrite(ptr, size, nmemb, stream);
}

size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
	char szBuff[256];

	sprintf(szBuff, "DBG> In my fread(%d, %lld, %lld).\n", stream->_fileno, (long int)size, (long int)nmemb);
	if(bDbg) write(1, szBuff, strlen(szBuff));

	if(real_fread==NULL)	{
		real_fread = (org_fread)dlsym(RTLD_NEXT, "fread");
	}

	return real_fread(ptr, size, nmemb, stream);
}
*/


static void Find_Func_Addr(char szPathLib[], char szFunc_List[][MAX_LEN_FUNC_NAME], long int func_addr[], long int func_len[], long int img_base_addr, int nhook)
{
	int fd, i, j, k, count=0;
	struct stat file_stat;
	void *map_start;
	Elf64_Sym *symtab;
	Elf64_Ehdr *header;
	Elf64_Shdr *sections;
	int strtab_offset=0;
	void *pSymbBase=NULL;
	int nSym=0, nSymTotal=0, SymRecSize=0, SymOffset, RecAddr;
	char *szSymName;

	
	stat(szPathLib, &file_stat);
	
	fd = open(szPathLib, O_RDONLY);
	if(fd == -1)    {
		printf("Fail to open file %s\nQuit\n", szPathLib);
		exit(1);
	}
	
	map_start = mmap(0, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if((long int)map_start == -1)   {
		printf("Fail to mmap file %s\nQuit\n", szPathLib);
		exit(1);
	}
	header = (Elf64_Ehdr *) map_start;
	
	sections = (Elf64_Shdr *)((char *)map_start + header->e_shoff);
	
	for (i = 0; i < header->e_shnum; i++)	{
		if ( (sections[i].sh_type == SHT_DYNSYM) || (sections[i].sh_type == SHT_SYMTAB) ) {
			pSymbBase = (void*)(sections[i].sh_offset + map_start);
			SymRecSize = sections[i].sh_entsize;
			nSym = sections[i].sh_size / sections[i].sh_entsize;
			
			for (j = i-1; j < i+2; j++)   {	// tricky here!!!
				if ( (sections[j].sh_type == SHT_STRTAB) ) {
					strtab_offset = (int)(sections[j].sh_offset);
				}
			}
			
			for(int j=0; j<nSym; j++) {
				RecAddr = SymRecSize*j;
				SymOffset = *( (int *)( pSymbBase + RecAddr ) ) & 0xFFFF;
				szSymName = (char *)( map_start + strtab_offset + SymOffset );
//				Type = *((int *)(pSymbBase + RecAddr + 4)) & 0xf;

				for(k=0; k<nhook; k++)	{
					if( strcmp(szSymName, szFunc_List[k])==0 )      { 
						func_addr[k] = ( (long int)( *((int *)(pSymbBase + RecAddr + 8)) ) & 0xFFFFFFFF ) + img_base_addr;
						func_len[k] = *((int *)(pSymbBase + RecAddr + 16));
//						count++;
					}
				}
				
//				printf("%-20s addr %x \n", szSymName, *((int *)(pSymbBase + RecAddr + 8)));
				nSymTotal++;
			}
		}
	}
/*	
	for (i = 0; i < header->e_shnum; i++)   {
		if ( (sections[i].sh_type == SHT_STRTAB) ) {
			strtab_offset = (int)(sections[i].sh_offset);
			break;
		}
	}
	
	for (i = 0; i < header->e_shnum; i++)   {
		if ( (sections[i].sh_type == SHT_DYNSYM) || (sections[i].sh_type == SHT_SYMTAB) ) {
			pSymbBase = (void*)(sections[i].sh_offset + map_start);
			SymRecSize = sections[i].sh_entsize;
			nSym = sections[i].sh_size / sections[i].sh_entsize;
			break;
		}
	}
	
	for(i=0; i<nSym; i++) {
		RecAddr = SymRecSize*i;
		SymOffset = *( (int *)( pSymbBase + RecAddr ) ) & 0xFFFF;
		szSymName = (char *)( map_start + strtab_offset + SymOffset );
		
		
//		if(count == nhook)  {
//			break;
//		}
	}
*/
	munmap(map_start, file_stat.st_size);
	close(fd);

/*
	printf("In lib %s. %p\n", szPathLib, img_base_addr);
	for(j=0; j<nhook; j++)	{
		printf("Found %-16s at %llx with length %llx\n", szFunc_List[j], func_addr[j]-img_base_addr, func_len[j]);
	}
	printf("\n\n");
*/
}


static void Uninstall_Patches(void)
{
	int i;
	void *pbaseOrg;
	size_t MemSize_Modify;
	long int OrgEntryCode;
	char *pOpOrgEntry;

	for(i=0; i<NHOOK; i++)	{
		pbaseOrg = (void *)( (long int)(pTrampoline[i].pOrgFunc) & filter );	// fast mod
		MemSize_Modify = Set_Block_Size((void *)(pTrampoline[i].pOrgFunc));
		
		if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{	// two pages to make sure the code works when the modified code is around page boundary
			printf("Error in executing p_mp().\n");
			exit(1);
		}
		OrgEntryCode = *((long int*)(pTrampoline[i].pOrgFunc));
		memcpy(&OrgEntryCode, pTrampoline[i].trampoline, 5);	// 5 bytes
		*( (long int *)(pTrampoline[i].pOrgFunc) ) = OrgEntryCode;;
		if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
			printf("Error in executing p_mp().\n");
			exit(1);
		}
	}
	
	if(func_addr_in_lib_pthread[0])	{
		for(i=0; i<NHOOK_IN_PTHREAD; i++)	{
			pbaseOrg = (void *)( func_addr_in_lib_pthread[i] & filter );	// fast mod
			
			MemSize_Modify = Set_Block_Size((void *)(func_addr_in_lib_pthread[i]));
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s in libpthread\n", szFunc_List[i]);
				perror("mprotect");
				exit(1);
			}
			*( (long int *)(func_addr_in_lib_pthread[i]) ) = org_code_saved_lib_pthread[i];	// restore the orginal code
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s in libpthread\n", szFunc_List[i]);
				exit(1);
			}
		}
	}
	
}

static int callback(struct dl_phdr_info *info, size_t size, void *data)
{
	if(strstr(info->dlpi_name, "/libc.so"))	{
		strcpy(szPathLibc, info->dlpi_name);
		img_libc_base = (info->dlpi_addr + info->dlpi_phdr[0].p_vaddr) & filter;
	}
	else if(strstr(info->dlpi_name, "/libpthread.so"))	{
		strcpy(szPathLibpthread, info->dlpi_name);
		img_libpthread_base = (info->dlpi_addr + info->dlpi_phdr[0].p_vaddr) & filter;
		IsLibpthreadLoaded = 1;
	}
	
	return 0;
}

static __attribute__((constructor)) void init_hook()
{
	int i;
	char szUid[16];
	void *p_patch_allocated=NULL;

	for(i=0; i<NHOOK_IN_PTHREAD; i++)	{
		func_addr_in_lib_pthread[i] = 0;
	}

	Determine_BCast_Dir();

	page_size = sysconf(_SC_PAGESIZE);
	filter = ~(page_size - 1);

	dl_iterate_phdr(callback, NULL);	// get library full path. ~44 us

	uid = getuid();
	sprintf(szUid, "_%d", uid);
	strcat(szRoot, szUid);

	init_shm();

//	DictSearch(p_Hash_File, "abcdefg", &elt_list_file, &ht_table_file);
//	DictSearch(p_Hash_File, "ILSVRC2012_img_train/n02096051/n02096051_3045.JPEG", &elt_list_file, &ht_table_file);;
//	printf("%d\n", DictSearch(p_Hash_File, "ILSVRC2012_img_train/n02096051/n02096051_3045.JPEG"));

	memset(FileStatus, 0, sizeof(FILESTATUS)*MAX_OPENED_FILE);


	Get_Module_Maps();
	Find_Func_Addr(szPathMyLib, szFunc_List, func_addr, func_len, img_mylib_base, NHOOK);	// get addresses and function length for my new functions.
	Find_Func_Addr(szPathLibc, szOrgFunc_List, org_func_addr, org_func_len, img_libc_base, NHOOK);	// get addresses and function length for libc functions.
	Find_Func_Addr(szPathLibpthread, szOrgFunc_List, func_addr_in_lib_pthread, func_len_in_lib_pthread, img_libpthread_base, NHOOK_IN_PTHREAD);	// get addresses and function length for libc functions.

//	Query_Original_Func_Address();	// get addresses of org functions

	p_patch = Query_Available_Memory_BLock((void*)(org_func_addr[0]));
	

	p_patch_allocated = mmap(p_patch, MIN_MEM_SIZE, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if(p_patch_allocated == MAP_FAILED) {
		printf("Fail to allocate code block at %p with mmap().\nQuit\n", p_patch);
		exit(1);
	}
	else if(p_patch_allocated != p_patch) {
		printf("Allocated at %p. Desired at %p\n", p_patch_allocated, p_patch);
	}

	pTrampoline = (TRAMPOLINE *)p_patch;
	Init_udis86();
	Setup_Trampoline();
	
	Inited = 1;
}

static __attribute__((destructor)) void finalize()
{
	int i;

	if(Inited)	{
//		for(i=0; i<MAX_OPENED_FILE; i++)	{
//			if( FileStatus[i].pData ) enqueue(FileStatus[i].idx, QUEUE_TAG_CLOSE);	// close all currently opened files
//		}

		Uninstall_Patches();
		
		if ( munmap(p_patch, MIN_MEM_SIZE) ) {
			perror("munmap");
		}
	}
}


void init_shm(void)
{
	struct stat file_stat;
	char szName[256], szMsg[256];
	char szHostname[256];
	
	gethostname(szHostname, 256);
	shm_ht_fd = shm_open(szNameHTShm, O_RDONLY, 0600);
	if(shm_ht_fd == -1)    {	// failed to create
		sprintf(szMsg, "Error to create %s on %s\nQuit\n", szNameHTShm, szHostname);
		perror(szMsg);
		exit(1);
	}
	nSize_Shared_Memory_HT = sizeof(struct dict) + sizeof(int)*INITIAL_SIZE + sizeof(struct elt)*INITIAL_SIZE; // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	p_shm_ht = mmap(NULL, nSize_Shared_Memory_HT, PROT_READ, MAP_SHARED, shm_ht_fd, 0);
	if (p_shm_ht == MAP_FAILED) {
		perror("mmap failed for p_shm_ht");
	}
	
	p_Hash_File = (struct dict *)p_shm_ht;
	DictCreate(p_Hash_File, 0, &elt_list_file, &ht_table_file);	// Not initialization
//	DictCreate(p_Hash_File, 0);	// Not initialization

//	DictCreate();	// init hash table. Already initialized


	shm_ht_dir_fd = shm_open(szNameHTDirShm, O_RDONLY, 0600);
	if(shm_ht_dir_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameHTDirShm);
		exit(1);
	}
	sprintf(szName, "/dev/shm/%s", szNameHTDirShm);
	lstat(szName, &file_stat);
	nSize_Shared_Memory_HT_Dir = file_stat.st_size;
	p_shm_ht_dir = mmap(NULL, nSize_Shared_Memory_HT_Dir, PROT_READ, MAP_SHARED, shm_ht_dir_fd, 0);
	if (p_shm_ht_dir == MAP_FAILED) {
		perror("mmap failed for p_shm_ht_dir");
	}
	
	p_Hash_Dir = (struct dict *)p_shm_ht_dir;
	DictCreate(p_Hash_Dir, 0, &elt_list_dir, &ht_table_dir);	// Not initialization

	nDir = p_Hash_Dir->n;	// hash table record number. # of dir
	p_DirRec = (DIRREC *)(p_shm_ht_dir + sizeof(struct dict) + sizeof(int)*nDir*2 + sizeof(struct elt)*nDir*2);
	p_DirInfo = (unsigned char *)(p_shm_ht_dir + sizeof(struct dict) + sizeof(int)*nDir*2 + sizeof(struct elt)*nDir*2 + sizeof(DIRREC)*nDir);

//	for(int i=0; i<nDir; i++)	{
//		printf("Found %d entries in directory %s\n", p_DirRec[i].nEntry, p_DirRec[i].szDirName);
//		for(int j=0; j<p_DirRec[i].nEntry; j++)	{
//			printf("      %3d %s\n", j, p_DirInfo + p_DirRec[i].addr + j*MAX_DIR_ENTRY_LEN);	// print each entry
//		}
//	}

	// start the shared memory for io job queue
	shm_queue_fd = shm_open(szNameQueueShm, O_RDWR, 0600);
	if(shm_queue_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameQueueShm);
		exit(1);
	}

	nSize_Shared_Memory_Queue = sizeof(pthread_mutex_t) + sizeof(long int)*2 + sizeof(int)*1 + sizeof(long int)*QUEUE_SIZE;
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
	shm_meta_fd = shm_open(szNameMetaShm, O_RDONLY, 0600);
	if(shm_meta_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameMetaShm);
		exit(1);
	}

	nSize_Shared_Memory_Meta = 16 + sizeof(FILEREC)*MAX_FILE + sizeof(struct stat)*MAX_FILE;
	p_shm_meta = mmap(NULL, nSize_Shared_Memory_Meta, PROT_READ, MAP_SHARED, shm_meta_fd, 0);
	if (p_shm_meta == MAP_FAILED) {
		perror("mmap failed for p_shm_meta");
	}

	p_nFile = (int *) p_shm_meta;
	p_myrank = (int *) (p_shm_meta + sizeof(int));
	p_bUsePack = (int *) (p_shm_meta + sizeof(int)*2);
	p_MetaData = (FILEREC *) (p_shm_meta + 16);
	p_stat = (struct stat *) (p_shm_meta + 16 + sizeof(FILEREC)*MAX_FILE);
	// end the shared memory for meta data info

	// start the shared memory for file buffer
	shm_file_buff_fd = shm_open(szNameFileBuffShm, O_RDONLY, 0600);
	if(shm_file_buff_fd == -1)    {	// failed to create
		printf("Error to create %s\nQuit\n", szNameFileBuffShm);
		exit(1);
	}

	nSize_Shared_Memory_File_Buff = sizeof(char)*pool_size_local;
	p_shm_file_buff = mmap(NULL, nSize_Shared_Memory_File_Buff, PROT_READ, MAP_SHARED, shm_file_buff_fd, 0);
	if (p_shm_file_buff == MAP_FAILED) {
		perror("mmap failed for p_shm_meta");
	}

	p_File_Buff = (unsigned char *) p_shm_file_buff;
	// end   the shared memory for file buffer
}


inline void enqueue(int file_idx, int Op)
{
	long int Tag;

	Tag = ( ((long int)Op ) << 32 ) |  ( (long int) file_idx ) ;

	while( ( *p_back - *p_front ) >= QUEUE_FULL )	{	// Queue is full. block until queue has enough space
		printf("Queued_Job_List is FULL.\n");
	}

  if (pthread_mutex_lock(p_queue_lock) != 0) {
    perror("pthread_mutex_lock");
    exit(2);
  }
  (*p_back)++;
  Queued_Job_List[ (*p_back) & QUEUE_FULL] = Tag;
  if (pthread_mutex_unlock(p_queue_lock) != 0) {
    perror("pthread_mutex_unlock");
    exit(2);
  }
  
//  long int ts;
//  ts = rdtscp();
//  printf("DBG> enqueue %lld  (%d,%d) time = %lld\n", *p_back, file_idx, Op, ts);
}

inline long int dequeue(void)
{
	long int ret;
  if (pthread_mutex_lock(p_queue_lock) != 0) {
    perror("pthread_mutex_lock");
    exit(2);
  }
  ret = *p_front;
  (*p_front)++;
  if (pthread_mutex_unlock(p_queue_lock) != 0) {
    perror("pthread_mutex_unlock");
    exit(2);
  }

  ret = Queued_Job_List[ret & QUEUE_FULL];

  return ret;
}

int Find_Next_Available_fd(int idx)
{
	int i;

	for(i=0; i<MAX_OPENED_FILE; i++)	{
		if(FileStatus[idx & MAX_OPENED_FILE_M1].pData == 0)	{	// Not used
			Next_fd = idx & MAX_OPENED_FILE_M1;
			return Next_fd;
		}
		idx++;
	}
	printf("Fail to find an unused file status element.\nQuit\n");
	exit(1);

	return -1;
}

void Determine_BCast_Dir(void)
{
	char *szEnv_BCast, *szEnv_fs_Root;
	int nLenRootDir;

	szEnv_BCast=getenv("DIR_BCAST");
	if(szEnv_BCast == NULL)	{
		szBCastDir[0] = 0;	// null string
	}
	else	{
		strncpy(szBCastDir, szEnv_BCast, 128);
	}

	szEnv_fs_Root=getenv("FS_ROOT");
	if(szEnv_fs_Root != NULL)	{
		strncpy(szRoot, szEnv_fs_Root, 128);
		nLenRootDir = strlen(szRoot); 
		if(szRoot[nLenRootDir-1] == '/')	{
			szRoot[nLenRootDir-1] = 0;	// remove the last '/'
		}
	}

	sprintf(szScatterDir, "%s/", szRoot);	// generate szScatterDir from szRoot

	nLen_BCastDir = strlen(szBCastDir);
	nLen_ScatterDir = strlen(szScatterDir);

	if( (szBCastDir[nLen_BCastDir-1] != '/') && (nLen_BCastDir>1) )	{	// append "/"
		strcat(szBCastDir, "/");
		nLen_BCastDir++;
	}
}

int lgetfilecon(const char *path, char *con)
{
	errno = 0x5f;
	return -1;
}

int acl_extended_file(const char *path_p)
{
	errno = 0x3d;
	return 0;
}


#define MAX_MAP_SIZE	(524288)

void Get_Module_Maps(void)
{
	FILE *fIn;
	char szName[64], szBuf[MAX_MAP_SIZE], szLibName[256];
	int i, iPos, ReadItem, Idx, pid;
	long int FileSize;
	uint64_t addr_B, addr_E, addr_my_code;
	
	addr_my_code = (uint64_t)init_hook;
	
	pid = getpid();
	sprintf(szName, "/proc/%d/maps", pid);
	fIn = fopen(szName, "rb");	// non-seekable file. fread is needed!!!
	if(fIn == NULL)	{
		printf("Fail to open file: %s\nQuit\n", szName);
		exit(1);
	}
	
	FileSize = fread(szBuf, 1, MAX_MAP_SIZE, fIn);	// fread can read complete file. read() does not most of time!!!
	fclose(fIn);
	
	szBuf[FileSize] = 0;
	
	iPos = 0;	// start from the beginging
	
	while(iPos >= 0)	{
		ReadItem = sscanf(szBuf+iPos, "%lx-%lx", &addr_B, &addr_E);
		if(ReadItem == 2)	{
			Addr_Min[nSeg] = addr_B;
			Addr_Max[nSeg] = addr_E;
			if(nSeg > 1)	{	// merge contacted blocks !!!
				if(Addr_Min[nSeg] == Addr_Max[nSeg-1])	{
					Addr_Max[nSeg-1] = Addr_Max[nSeg];
					nSeg--;
				}
			}
			if( (addr_my_code >=addr_B) && (addr_my_code <=addr_E) && (img_mylib_base == 0) )	{
				ReadItem = sscanf(szBuf+iPos+72, "%s", szLibName);
				if(ReadItem == 1)	{
					if(strlen(szLibName) > 3)	{
						strcpy(szPathMyLib, szLibName);
						img_mylib_base = (unsigned long int)addr_B;
//						printf("My code is in %s. (%p, %p)\n", szPathMyLib, (void*)addr_B, (void*)addr_E);
					}
				}
			}
			nSeg++;
		}
		iPos = Get_Position_of_Next_Line(szBuf, iPos + 38, FileSize);	// find the next line
	}
	
}

void* Query_Available_Memory_BLock(void *p)
{
	int i, iSeg;
	uint64_t pCheck=(uint64_t)p;
	void *p_Alloc;

	iSeg = -1;
	for(i=0; i<nSeg; i++)	{
		if( (pCheck >= Addr_Min[i]) && (pCheck <= Addr_Max[i]) )	{
			iSeg = i;
			break;
		}
	}

	if(iSeg < 0)	{
		printf("p_Query = %p\n", p);
		printf("Something wrong! The address you queried is not inside any module!\nQuit\n");
		sleep(60);
		exit(1);
	}
	p_Alloc = (void*)(Addr_Max[iSeg]);

	if( iSeg < (nSeg - 1) )	{
		if( (Addr_Min[iSeg+1] - Addr_Max[iSeg]) < MIN_MEM_SIZE)	{
			printf("Only %llx bytes available.\nQuit\n", Addr_Min[iSeg+1] - Addr_Max[iSeg]);
			exit(1);
		}
	}

//	printf("p_Alloc = %p  Distance = %ld\n", p_Alloc, Addr_Max[iSeg] - pCheck);

	return p_Alloc;
}


static int Get_Position_of_Next_Line(char szBuff[], int iPos, int nBuffSize)
{
	int i=iPos;
	
	while(i < nBuffSize)	{
		if(szBuff[i] == 0xA)	{	// A new line
			i++;
			return ( (i>=nBuffSize) ? (-1) : (i) );
		}
		else	{
			i++;
		}
	}
	return (-1);
}

static void Query_Original_Func_Address(void)
{
	void *module;
	int i;

	module = dlopen(szPathLibc, RTLD_LAZY);
	if(module == NULL)	{
		printf("Fail to dlopen: %s.\n", szPathLibc);
		return;
	}

	for(i=0; i<NHOOK; i++)	{
		org_func_addr[i] = (long int)dlsym(module, szOrgFunc_List[i]);
		if(org_func_addr[i] == 0)	{
			printf("Fail to obtain the address of function: %s in file %s\nQuit\n", szOrgFunc_List[i], szPathLibc);
			exit(1);
		}
	}

	dlclose(module);

	// open and close in libpthread should be used if applicable.
	if(IsLibpthreadLoaded)	{
		if(abs(img_libc_base - img_libpthread_base) > 0x7FFFFFFF)	{	// too far away!!!
			printf("libc and libpthread are too far away!\nQuit\n");
			exit(1);
		}

		module = dlopen(szPathLibpthread, RTLD_LAZY);
		if(module == NULL)	{
			printf("Fail to dlopen: %s.\n", szPathLibpthread);
			return;
		}
		
		for(i=0; i<NHOOK_IN_PTHREAD; i++)	{	// open and close
			func_addr_in_lib_pthread[i] = (long int)dlsym(module, szOrgFunc_List[i]);	// "open"
			if(func_addr_in_lib_pthread[i] == 0)	{
				printf("Fail to obtain the address of function: %s in file %s\nQuit\n", szOrgFunc_List[i], szPathLibpthread);
				exit(1);
			}
		}
		
		dlclose(module);
	}
}

inline static size_t Set_Block_Size(void *pAddr)
{
	unsigned long int res, pOrg;

	pOrg = (unsigned long int)pAddr;
	res = pOrg % page_size;
	if( (res + 5) > page_size )	{	// close to the boundary of two memory pages 
		return (size_t)(page_size*2);
	}
	else	{
		return (size_t)(page_size);
	}
}

static void Setup_Trampoline(void)
{
	int i, j, jMax, ReadItem, *p_int, WithJmp[NHOOK];
	int nInstruction, RIP_Offset, Jmp_Offset;
	int OffsetList[MAX_INSTUMENTS];
	char szHexCode[MAX_INSTUMENTS][32], szInstruction[MAX_INSTUMENTS][64];
	char *pSubStr=NULL, *pOpOrgEntry;
	long int OrgEntryCode;
	void *pbaseOrg;
	size_t MemSize_Modify;

	Max_Bytes_Disassemble = 24;
	for(i=0; i<NHOOK; i++)	{	// disassemble the orginal functions
		WithJmp[i]=-1;

		pTrampoline[i].pOrgFunc = (void *)(org_func_addr[i]);
		pTrampoline[i].Offset_RIP_Var = NULL_RIP_VAR_OFFSET;
		pTrampoline[i].nBytesCopied = 0;

		ud_obj.pc = 0;
		ud_set_input_hook(&ud_obj, input_hook_x);
		
		nInstruction = 0;
		ud_idx = 0;
		szOp = (char *)(org_func_addr[i]);

//		printf("Disassemble %s\n", szOrgFunc_List[i]);
		while (ud_disassemble(&ud_obj)) {
			OffsetList[nInstruction] = ud_insn_off(&ud_obj);
			if(OffsetList[nInstruction] >= MAX_JMP_LEN)	{	// size of jmp instruction
				pTrampoline[i].nBytesCopied = OffsetList[nInstruction];
				if( (nInstruction > 0) && (strncmp(szHexCode[nInstruction-1], "e9", 2)==0) )	{
					if(strlen(szHexCode[nInstruction-1]) == 10)	{	// found a jmp instruction here!!!
						if(nInstruction >= 2)	{
							WithJmp[i] = strlen(szHexCode[nInstruction-2])/2 + 1;
						}
					}
				}
				break;
			}
//			printf("%16x ", OffsetList[nInstruction]);        // instruction offset
			
			strcpy(szHexCode[nInstruction], ud_insn_hex(&ud_obj));
			strcpy(szInstruction[nInstruction], ud_insn_asm(&ud_obj));
//			printf("%-16.16s %-24s", szHexCode[nInstruction], szInstruction[nInstruction]);   // binary code, assembly code
//			printf("\n");
			pSubStr = strstr(szInstruction[nInstruction], "[rip+");
			if(pSubStr)	{
				ReadItem = sscanf(pSubStr+5, "%x]", &RIP_Offset);
				if(ReadItem == 1)	{
					pTrampoline[i].Offset_RIP_Var = RIP_Offset;
//					printf("RIP_Offset = %x\n", RIP_Offset);
				}
			}
			nInstruction++;
		}
//		printf("To copy %d bytes.\n\n", nByteToCopied);
	}

	for(i=0; i<NHOOK; i++)	{
		memcpy(pTrampoline[i].bounce, szOp_Bounce, BOUNCE_CODE_LEN);
		*((long int *)(pTrampoline[i].bounce + OFFSET_HOOK_FUNC)) = func_addr[i];
	}


	for(i=0; i<NHOOK; i++)	{
		memcpy(pTrampoline[i].trampoline, pTrampoline[i].pOrgFunc, pTrampoline[i].nBytesCopied);
		pTrampoline[i].trampoline[pTrampoline[i].nBytesCopied] = 0xE9;	// jmp
//		Jmp_Offset = (int) ( ( (long int)(pTrampoline[i].pOrgFunc) + pTrampoline[i].nBytesCopied - ( (long int)(pTrampoline[i].trampoline) +  pTrampoline[i].nBytesCopied + 5) )  & 0xFFFFFFFF);
		Jmp_Offset = (int) ( ( (long int)(pTrampoline[i].pOrgFunc) - ( (long int)(pTrampoline[i].trampoline) + 5) )  & 0xFFFFFFFF);
		*((int*)(pTrampoline[i].trampoline + pTrampoline[i].nBytesCopied + 1)) = Jmp_Offset;

		if(pTrampoline[i].Offset_RIP_Var != NULL_RIP_VAR_OFFSET)	{
			jMax = pTrampoline[i].nBytesCopied - 4;
			for(j=1; j<jMax; j++)	{
				p_int = (int *)(pTrampoline[i].trampoline + j);
				if( *p_int == pTrampoline[i].Offset_RIP_Var )	{
					*p_int += ( (int)( ( (long int)(pTrampoline[i].pOrgFunc) - (long int)(pTrampoline[i].trampoline) )  ) );	// correct relative offset of PIC var
				}
			}
		}
		if(WithJmp[i] > 0)	{
//			printf("pTrampoline[i].nBytesCopied = %d WithJmp = %d\n", pTrampoline[i].nBytesCopied, WithJmp[i]);
			p_int = (int *)(pTrampoline[i].trampoline + WithJmp[i]);
//			printf("Before: %x\n", *p_int);
			*p_int += ( (int)( ( (long int)(pTrampoline[i].pOrgFunc) - (long int)(pTrampoline[i].trampoline) )  ) );
//			printf("After: %x\n", *p_int);
//			printf("Fixed jmp instruction offset in function: %s\n", szOrgFunc_List[i]);
		}
	}
	
	// set up function pointers for original functions

	real_open = (org_open)(pTrampoline[0].trampoline);
	real_close_nocancel = (org_close_nocancel)(pTrampoline[1].trampoline);
	real_read = (org_read)(pTrampoline[2].trampoline);
	real_lseek = (org_lseek)(pTrampoline[3].trampoline);
	real_fxstat = (org_fxstat)(pTrampoline[4].trampoline);
	
	//start to patch orginal function
	for(i=0; i<NHOOK; i++)	{
		pbaseOrg = (void *)( (long int)(pTrampoline[i].pOrgFunc) & filter );	// fast mod

		MemSize_Modify = Set_Block_Size((void *)(pTrampoline[i].pOrgFunc));
		if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{
			printf("Error in executing mprotect(). %s\n", szFunc_List[i]);
			exit(1);
		}

		OrgEntryCode = *((long int*)(pTrampoline[i].pOrgFunc));
		pOpOrgEntry = (char *)(&OrgEntryCode);
		pOpOrgEntry[0] = 0xE9;
		*((int *)(pOpOrgEntry+1)) = (int)( (long int)(pTrampoline[i].bounce) - (long int)(pTrampoline[i].pOrgFunc) - 5 );
		*( (long int *)(pTrampoline[i].pOrgFunc) ) = OrgEntryCode;

		if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
			printf("Error in executing mprotect(). %s\n", szFunc_List[i]);
			exit(1);
		}
	}


	// patch functions in libpthread if applicable
	if(func_addr_in_lib_pthread[0])	{
		for(i=0; i<NHOOK_IN_PTHREAD; i++)	{
			pbaseOrg = (void *)( func_addr_in_lib_pthread[i] & filter );	// fast mod
			
			MemSize_Modify = Set_Block_Size((void *)(func_addr_in_lib_pthread[i]));
//			printf("pbaseOrg = %p  MemSize_Modify = %llx ", pbaseOrg, (long int)MemSize_Modify);
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s in libpthread\n", szFunc_List[i]);
				perror("mprotect");
				exit(1);
			}
			
			OrgEntryCode = *((long int*)(func_addr_in_lib_pthread[i]));
			org_code_saved_lib_pthread[i] = OrgEntryCode;
			pOpOrgEntry = (char *)(&OrgEntryCode);
			pOpOrgEntry[0] = 0xE9;
			*((int *)(pOpOrgEntry+1)) = (int)( (long int)(pTrampoline[i].bounce) - func_addr_in_lib_pthread[i] - 5 );
			*( (long int *)(func_addr_in_lib_pthread[i]) ) = OrgEntryCode;
			
			if(mprotect(pbaseOrg, MemSize_Modify, PROT_READ | PROT_EXEC) != 0)	{
				printf("Error in executing mprotect(). %s in libpthread\n", szFunc_List[i]);
				exit(1);
			}
		}
	}

	
}


static void Init_udis86(void)
{
	ud_init(&ud_obj);
	ud_set_mode(&ud_obj, 64);     // 32 or 64
	ud_set_syntax(&ud_obj, UD_SYN_INTEL); // intel syntax
}

int input_hook_x(ud_t* u)
{
	if(ud_idx < Max_Bytes_Disassemble)        {
		ud_idx++;
		return (int)(szOp[ud_idx-1] & 0xFF);
	}
	else    {
		return UD_EOI;
	}
}

