/*
Written by Lei Huang (huang@tacc.utexas.edu)
       and Zhao Zhang (zzhang@tacc.utexas.edu) at TACC. 
All rights are reserved.
*/

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <dirent.h>
#include <pthread.h>
#include <errno.h>

#include "lzsse8.h"

#define MAX_WORKER   (512)

#define MAX_NAME_LEN	(256)
//#define MAX_FILE	(1024*1024*2)
#define MAX_DIR		(128*1024)
#define MAX_FILE_SIZE	(512*1024*1024)
#define LEN_REC		(320)
#define MAX_PARTITION	(2048)
#define MAX_DIR_ENTRY_LEN	(64)
//#define MAX_ENTRY_PER_DIR	(65536)
#define MAX_ENTRY_PER_DIR	(327680)

long int PACK_TAG=0x4741545F4B434150;	// tag string "PACK_TAG"

#ifndef MY_MIN
#define MY_MIN
#define min(a,b) ( ((a)<(b)) ? ((a)) : ((b)) )
#endif

int nPartition=1;
int nLineRec=0;

//char szFileName[MAX_FILE][MAX_NAME_LEN];
//struct stat stat_list[MAX_FILE];
char **szFileName, *p_szFileName=NULL;
struct stat *stat_list;

char szEntryList[MAX_ENTRY_PER_DIR][MAX_DIR_ENTRY_LEN];
char szDirName[MAX_DIR][MAX_NAME_LEN];
int nFile_in_Partition[MAX_PARTITION];
unsigned char szData[MAX_FILE_SIZE], szDataPacked[MAX_FILE_SIZE], szDataUnpacked[MAX_FILE_SIZE];

typedef struct	{
	int workerid;
	int nFile_per_Partition, nLen_File_Name, nLen_stat, Pack_Level;
}PARAM;

#define OUT_OF_SPACE	(-111)

ssize_t read_all(int fd, void *buf, size_t count);
ssize_t write_all(int fd, const void *buf, size_t count);

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
void *Job_IO(void *void_ptr)
{
	PARAM *pParam = (PARAM *)void_ptr;
	int workerid, nFile_per_Partition, nLen_File_Name, nLen_stat, Pack_Level;
	int j, offset, idx;
	int fd, fd_rd;
	char szOutputName[256];
	unsigned char *szDataLocal, *szDataPacked;
	LZSSE8_OptimalParseState *pState;
	long int nBytesPacked, nBytesUnpacked;

	workerid = pParam->workerid;
	nFile_per_Partition = pParam->nFile_per_Partition;
	nLen_File_Name = pParam->nLen_File_Name;
	nLen_stat = pParam->nLen_stat;
	Pack_Level = pParam->Pack_Level;
	
	szDataLocal = (unsigned char *)malloc(MAX_FILE_SIZE);
	if(szDataLocal == NULL)	{
		printf("Fail to allocate memory of %d\nQuit\n", MAX_FILE_SIZE);
		exit(1);
	}
	szDataPacked = (unsigned char *)malloc(MAX_FILE_SIZE);
	if(szDataPacked == NULL)	{
		printf("Fail to allocate memory of %d\nQuit\n", MAX_FILE_SIZE);
		exit(1);
	}

	offset = nFile_per_Partition * workerid;
	sprintf(szOutputName, "fs_%d", workerid);
	fd = open(szOutputName, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
	write_all(fd, &nFile_per_Partition, sizeof(int));	// number of files in this partition
	
	for(j=0; j<nFile_per_Partition; j++)	{
		idx = offset + j;
		
		fd_rd = open(szFileName[idx], O_RDONLY);
		if(fd_rd == -1)	{
			printf("Fail to open file: %s\nQuit\n", szFileName[idx]);
		}
		
		read_all(fd_rd, szDataLocal, stat_list[idx].st_size);
		close(fd_rd);
		
		write_all(fd, szFileName[idx], nLen_File_Name);
		write_all(fd, &(stat_list[idx]), nLen_stat);

		if(Pack_Level > 0)	{	// store packed data
			if(stat_list[idx].st_size)	{
				pState = LZSSE8_MakeOptimalParseState(stat_list[idx].st_size);
//				memset(szDataPacked, 0, MAX_FILE_SIZE);
				
				nBytesPacked = LZSSE8_CompressOptimalParse(pState, szDataLocal, stat_list[idx].st_size, szDataPacked, MAX_FILE_SIZE, Pack_Level);
				nBytesUnpacked = LZSSE8_Decompress(szDataPacked, nBytesPacked, szDataUnpacked, MAX_FILE_SIZE);

				if( (nBytesUnpacked != stat_list[idx].st_size) || (nBytesPacked > stat_list[idx].st_size) )	{	// fail to unpack or no benefit to pack
					printf("Warning: No packing for file %s\n", szFileName[idx]);
					nBytesPacked = 0;
					write_all(fd, &nBytesPacked, sizeof(long int));
					write_all(fd, szDataLocal, stat_list[idx].st_size);
				}
				else	{
					write_all(fd, &nBytesPacked, sizeof(long int));
					write_all(fd, szDataPacked, nBytesPacked);
				}

				LZSSE8_FreeOptimalParseState(pState);
			}
			else	{
				nBytesPacked = 0;
				write_all(fd, &nBytesPacked, sizeof(long int));
			}
		}
		else	{	// unpacked. Original data
			nBytesPacked = 0;
			write_all(fd, &nBytesPacked, sizeof(long int));
			write_all(fd, szDataLocal, stat_list[idx].st_size);
		}

	}
	
	close(fd);
	
	printf("Finished partition %d.\n", workerid);

	free(szDataLocal);
	free(szDataPacked);
}

int main(int argc, char *argv[])
{
	int nFile_BCast=0, nFile=0, nDir=0;
	int i, j, fd, fd_rd, fd_bcast=-1, nLen, Partition_Max, offset, idx, nFileLeft;
	FILE *fIn, *fDir;
	char szLine[512], *ReadLine, *szBuff, szOutputName[256], szInput[256], szBCastTag[64];
	struct stat file_stat;
	pthread_t threadlist[MAX_WORKER];
	LZSSE8_OptimalParseState *pState;
	long int nBytesPacked, nBytesUnpacked;
	int nFile_per_Partition, nLen_File_Name, nLen_stat, Pack_Level=0;
	PARAM Param[MAX_WORKER];

	if( (argc < 3) || (argc > 5) )	{
		printf("Usage: prep n_partition list [bcast_dir] [pack_level_#]\nExample: prep 24 train validation pack_10\n");
		exit(1);
	}

	printf("argc = %d\n", argc);

	szBCastTag[0] = 0;

	nPartition = atoi(argv[1]);
	strncpy(szInput, argv[2], 256);
	if(argc >= 4)	{
		strncpy(szBCastTag, argv[3], 63);
		if(strcmp(szBCastTag, "NULL")==0)	{
			szBCastTag[0] = 0;
		}
		if(argc==5)	{
			if(strncmp(argv[4], "pack_", 5)==0)	{
				Pack_Level = atoi(argv[4]+5);
				if(Pack_Level <= 0)	{
					printf("Not a valid pack level. %d from %s.\n", Pack_Level, argv[4]);
					Pack_Level = 0;
				}
				else	{
					printf("Compressing level = %d\n", Pack_Level);
				}
			}
		}
	}

	nLen_stat = sizeof(struct stat);
	nLen_File_Name = LEN_REC - nLen_stat - sizeof(long int);	// store the packed file size!
//	nLen_File_Name = LEN_REC - nLen_stat;

	printf("LenStat = %d MAX_LEN_FILE = %d\n", nLen_stat, nLen_File_Name);

	fIn = fopen(szInput, "r");
	if(fIn == NULL)	{
		printf("Fail to open file %s\n", szInput);
		exit(1);
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
		if(nLen >= 1)	{
			nLineRec++;
		}
	}
	printf("Found %d records in file %s\n\n", nLineRec, szInput);

	// allocate memory
	stat_list = (struct stat *)malloc(sizeof(struct stat)*nLineRec);
	if(stat_list == NULL)	{
		printf("Fail to allocate memory for stat_list.\nQuit\n");
		fclose(fIn);
		exit(1);
	}

	szFileName = (char **)malloc(sizeof(char *)*nLineRec);
	if(szFileName == NULL)	{
		printf("Fail to allocate memory for szFileName\nQuit\n");
		exit(1);
	}
	p_szFileName = (char *)malloc(sizeof(char)*nLineRec*MAX_NAME_LEN);
	if(p_szFileName == NULL)	{
		printf("Fail to allocate memory for p_szFileName\nQuit\n");
		exit(1);
	}
	for(i=0; i<nLineRec; i++)	{
		szFileName[i] = p_szFileName + i*MAX_NAME_LEN;
	}

	

	fseek(fIn, 0, SEEK_SET);

	if(szBCastTag[0])	{
		fd_bcast = open("fs_bcast", O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
		if(fd_bcast == -1)	{
			printf("Fail to open file %s to write.\nQuit\n", "fs_bcast");
			exit(1);
		}
		write_all(fd_bcast, &nFile_BCast, sizeof(int));	// will be revised later
	}

	fDir = fopen("dir.list", "w");

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
		if(strcmp(szLine, ".") == 0)	{	// ignore
		}
		else if(strcmp(szLine, "dir.list") == 0)	{	// this file is generated by my code.
		}
		else {
			stat(szLine, &file_stat);
			if( (file_stat.st_mode & S_IFMT) == S_IFDIR )	{	// directory
				if( strncmp(szLine, "./", 2) == 0 )	{
					fprintf(fDir, "%s\n", szLine+2);
					strcpy(szDirName[nDir], szLine+2);
				}
				else	{
					fprintf(fDir, "%s\n", szLine);
					strcpy(szDirName[nDir], szLine);
				}
				file_stat.st_size = 0;	// set size for dir as zero
				nDir++;
				if(nDir > MAX_DIR)	{
					printf("nDir > MAX_DIR\nYou need to increase MAX_DIR\nQuit\n");
					exit(1);
				}
			}
//			else	{
				if(szBCastTag[0] && strstr(szLine, szBCastTag) )	{	// a file will be broadcasted to all nodes. Now dir is accounted as a file too!!!
					fd_rd = open(szLine, O_RDONLY);
					if(fd_rd == -1)	{
						printf("Fail to open file: %s\nQuit\n", szLine);
					}
					read_all(fd_rd, szData, file_stat.st_size);
					close(fd_rd);

					if( strncmp(szLine, "./", 2) == 0 ) 
						write_all(fd_bcast, szLine+2, nLen_File_Name);
					else
						write_all(fd_bcast, szLine, nLen_File_Name);

					if(Pack_Level > 0)	{	// store packed data
						if(file_stat.st_size)	{
							pState = LZSSE8_MakeOptimalParseState(file_stat.st_size);
//							memset(szDataPacked, 0, MAX_FILE_SIZE);
							nBytesPacked = LZSSE8_CompressOptimalParse(pState, szData, file_stat.st_size, szDataPacked, MAX_FILE_SIZE, Pack_Level);
							nBytesUnpacked = LZSSE8_Decompress(szDataPacked, nBytesPacked, szDataUnpacked, MAX_FILE_SIZE);

							if( (nBytesUnpacked != stat_list[idx].st_size) || ( (nBytesPacked+8) > stat_list[idx].st_size) )	{	// fail to unpack or no benefit to pack. Write the original file
//								printf("Warning: No packing for file %s\n", szFileName[idx]);
								write_all(fd_bcast, &(file_stat.st_size), sizeof(long int));
								write_all(fd_bcast, szData, file_stat.st_size);
							}
							else	{
								nBytesPacked += sizeof(long int);	// one tag is used at the very beginning of unpacked bcast file 
								write_all(fd_bcast, &nBytesPacked, sizeof(long int));
								write_all(fd_bcast, &PACK_TAG, sizeof(long int));	// write tag
								write_all(fd_bcast, szDataPacked, nBytesPacked-sizeof(long int));	// write packed content
							}
							
							LZSSE8_FreeOptimalParseState(pState);
						}
						else	{
							nBytesPacked = 0;
							write_all(fd_bcast, &nBytesPacked, sizeof(long int));
						}
					}
					else	{	// unpacked. Original data
						write_all(fd_bcast, &(file_stat.st_size), sizeof(long int));
						write_all(fd_bcast, szData, file_stat.st_size);
					}

					nFile_BCast++;
				}
				else	{
					if( strncmp(szLine, "./", 2) == 0 ) 
						strcpy(szFileName[nFile], szLine+2);
					else
						strcpy(szFileName[nFile], szLine);
					memcpy(&(stat_list[nFile]), &file_stat, nLen_stat);

					if(file_stat.st_size > MAX_FILE_SIZE)	{
						printf("file_stat.st_size > MAX_FILE_SIZE.\nfile_stat.st_size = %ld. You need to increase MAX_FILE_SIZE.\nQuit\n", (long int)(file_stat.st_size));
						exit(1);
					}

					nFile++;
//					if(nFile >= MAX_FILE)	{
//						printf("nFile >= MAX_FILE.\nYou need to increase MAX_FILE.\n");
//						exit(1);
//					}
				}
//			}
		}
	}

	if(fd_bcast > 0)	{
		lseek(fd_bcast, 0, SEEK_SET);
		write_all(fd_bcast, &nFile_BCast, sizeof(int));
		close(fd_bcast);
	}

	fclose(fDir);
	fclose(fIn);

	printf("Found %d files to scatter and %d files to broadcast.\n", nFile, nFile_BCast);

	if(nFile%nPartition == 0)	{
		nFile_per_Partition = nFile/nPartition;
	}
	else	{
		nFile_per_Partition = nFile/nPartition + 1;
	}

	Partition_Max = nFile/nFile_per_Partition;

	for(i=0; i<Partition_Max; i++)	{
		Param[i].workerid = i;
		Param[i].nFile_per_Partition = nFile_per_Partition;
		Param[i].nLen_File_Name = nLen_File_Name;
		Param[i].nLen_stat = nLen_stat;
		Param[i].Pack_Level = Pack_Level;
	}

	for(i=0; i<Partition_Max; i++)  {
		if(pthread_create(&(threadlist[i]), NULL, Job_IO, &(Param[i]))) {
			fprintf(stderr, "Error creating thread\n");
			return 1;
		}
	}

	if(nFile%nFile_per_Partition)	{
		offset = nFile_per_Partition * Partition_Max;
		nFileLeft = nFile%nFile_per_Partition;

		sprintf(szOutputName, "fs_%d", Partition_Max);
		fd = open(szOutputName, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
		write_all(fd, &nFileLeft, sizeof(int));	// number of files in this partition

		for(j=0; j<nFileLeft; j++)	{
			idx = offset + j;

			fd_rd = open(szFileName[idx], O_RDONLY);
			if(fd_rd == -1)	{
				printf("Fail to open file: %s\nQuit\n", szFileName[idx]);
			}

			read_all(fd_rd, szData, stat_list[idx].st_size);
			close(fd_rd);

			write_all(fd, szFileName[idx], nLen_File_Name);
			write_all(fd, &(stat_list[idx]), nLen_stat);

			if(Pack_Level > 0)	{	// store packed data
				if(stat_list[idx].st_size)	{
					pState = LZSSE8_MakeOptimalParseState(stat_list[idx].st_size);
//					memset(szDataPacked, 0, MAX_FILE_SIZE);
					nBytesPacked = LZSSE8_CompressOptimalParse(pState, szData, stat_list[idx].st_size, szDataPacked, MAX_FILE_SIZE, Pack_Level);
					nBytesUnpacked = LZSSE8_Decompress(szDataPacked, nBytesPacked, szDataUnpacked, MAX_FILE_SIZE);

					if( (nBytesUnpacked != stat_list[idx].st_size) || (nBytesPacked > stat_list[idx].st_size) )	{	// fail to unpack or no benefit to pack
						printf("Warning: No packing for file %s\n", szFileName[idx]);
						nBytesPacked = 0;
						write_all(fd, &nBytesPacked, sizeof(long int));
						write_all(fd, szData, stat_list[idx].st_size);
					}
					else	{
						write_all(fd, &nBytesPacked, sizeof(long int));
						write_all(fd, szDataPacked, nBytesPacked);
					}
					
					LZSSE8_FreeOptimalParseState(pState);
				}
				else	{
					nBytesPacked = 0;
					write_all(fd, &nBytesPacked, sizeof(long int));
				}
			}
			else	{	// unpacked. Original data
				nBytesPacked = 0;
				write_all(fd, &nBytesPacked, sizeof(long int));
				write_all(fd, szData, stat_list[idx].st_size);
			}
			
		}

		close(fd);
		printf("Finished partition %d.\n", Partition_Max);
	}

	int nEntry;
	DIR *dp;
	struct dirent *ep;

	fd = open("fs_dirinfo", O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
	if(fd == -1)	{
		printf("Fail to open file fs_dirinfo\nQuit\n");
		exit(1);
	}
	write_all(fd, &nDir, sizeof(int));
	for(i=0; i<nDir; i++)	{
		nEntry = 0;
		
		dp = opendir(szDirName[i]);
		if (dp != NULL)	{
			while (ep = readdir (dp))	{
				if( (strcmp(ep->d_name, ".")==0) || (strcmp(ep->d_name, "..")==0) )	{	// skip
				}
				else	{
					strcpy(szEntryList[nEntry], ep->d_name);
					nEntry++;
					if(nEntry > MAX_ENTRY_PER_DIR)	{
						printf("nEntry > MAX_ENTRY_PER_DIR\nYou need to increase MAX_ENTRY_PER_DIR\nQuit\n");
						exit(1);
					}
				}
//				printf("%s\n", ep->d_name);
			}
			closedir(dp);
			write_all(fd, szDirName[i], nLen_File_Name);	// full dir name
			write_all(fd, &nEntry, sizeof(int));
			write_all(fd, szEntryList[0], nEntry*MAX_DIR_ENTRY_LEN);
		}
		else
			perror ("Couldn't open the directory");
	}
	close(fd);


	for(i=0; i<Partition_Max; i++)	{
		if(pthread_join(threadlist[i], NULL)) {
			fprintf(stderr, "Error joining thread %d\n", i);
			return 2;
		}
	}

	free(stat_list);
	free(szFileName);
	free(p_szFileName);

	return 0;
}


