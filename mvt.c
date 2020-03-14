/* 	creator: S.  Umut Balkan
	date : 02/28/2020
	filename: mvt.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/wait.h>
#include <pthread.h>


int **ptr_result;
int vector_size;
int *ptr_arr;
int num_splits;

void *mapperThread(void *param){
    int row, col, val;
    FILE *fp,fp2;
    char filename[50];
    int num = (int *) param;
    sprintf(filename, "mvt-split%d.txt", num);
    printf("thread id: %ld\n file: %s\n", pthread_self(),filename);
    fp = fopen(filename, "r");

	if(fp == NULL){
		perror("main(): ");
		exit(1);
	}


    // create a result array from mvt-splitX.txt
	while(fscanf(fp, "%d%d%d", &row, &col, &val) == 3){
		ptr_result[num][row-1] += ptr_arr[col-1] * val;
	}

    for(int i = 0; i < vector_size; i++){
        printf(" %d ", ptr_result[num][i]);
    }


    printf("\n");

    

    pthread_exit(0);
}

void *reducerThread(void *param){
    FILE *fp;
    char filename[50];
    int *result_arr;
    result_arr = (int*) calloc(vector_size, sizeof(int));
    printf("\nThis is reducer\n");
    printf("%s ", (char *) param);
    printf("creating resultfile, in sorted order.\n");
    fp = fopen((char *) param,"w");
    	if(fp == NULL){
		perror("File Read/Write Error!\nmain(): ");
		exit(1);
	}
    for(int i = 0; i < num_splits; i++){
        for(int j = 0; j < vector_size; j++){
            result_arr[j] += ptr_result[i][j];
        }    
    }

	// create <resultfile>.txt
	fflush(stdout);
	for( int j = 0; j < vector_size; j++){
		fprintf(fp, "%d %d\n", j+1, result_arr[j]);
	}

	fclose(fp);

    printf("reducer terminating.\n");


    pthread_exit(0);
}


int main(int argc, char *argv[]){

    pthread_t* ptr_tid;
	pthread_t reducer_tid;
    int row, col, val;
	int num_lines;

    int s;
    FILE *fp,*fp2,*fp3;
	char filename[50];

    // checking num of arguments
	if (argc != 5){
		printf("wrong number of arguments\n");
		printf("usage: mvp <squarematrixfile.txt> <columnvector.txt> <resultfile.txt> <numberofsplits>\n");
		exit(1);
	}

	printf("MVP started.\n");

    fp = fopen(argv[1], "r"); // matrix-file
	fp2 = fopen(argv[2], "r"); // vector-file
    fp3 = fopen(argv[3], "w"); // result-file
	num_splits = atoi(argv[4]); // num-splits. K

    if(fp == NULL || fp2 == NULL || fp3 == NULL){
        perror("File Read/Write error\nmain(): ");
        exit(1);
    }

    if(num_splits <= 0 || num_splits > 10){
		printf("Error! Invalid number of splits.\nSplit value: %d\t", num_splits);
		printf("Split value should between [1,10].\n");
		exit(1);
	}

    // get the number of lines in matrixfile.txt
	num_lines = 0;
	while(fscanf(fp, "%d%d%d", &row, &col, &val) == 3){
		num_lines++;
	}
	printf("Matrix has %d non-zero entries\n", num_lines);

	// get the size of vectorfile.txt
	vector_size = 0;
	while(fscanf(fp2, "%d%d", &row, &val) == 2){
		vector_size++;
	}

    // calculating ceiling of L/K
	s = num_lines / num_splits;
	printf("s value: %d\n", s);

	// create mvt-splitX.txt files X = [0,num_splits-1]
	rewind(fp);
	for(int i = 0; i < num_splits; i++){
		fflush(stdout);
		// create files and write in it.
		sprintf(filename, "mvt-split%d.txt", i);
		fp2 = fopen(filename, "w");
		if(fp2 == NULL){
			perror("main(): ");
			exit(1);
		}
		int l = i*s;
		int p = l+s-1;
		if(i+1 == num_splits){
			int r = s * i;
			p += num_lines - r;
		}
		while(l <= p && fscanf(fp, "%d%d%d", &row, &col, &val) == 3){
			fprintf(fp2, "%d %d %d\n", row, col, val);;
			l++;
		}
		fclose(fp2);
	}
	fclose(fp);

    // allocate 2d result array.
    ptr_result = (int**) calloc(num_splits*vector_size, sizeof(int));
    for (int i = 0; i < num_splits; i++){
        ptr_result[i] = (int*) calloc(vector_size, sizeof(int));
    }
    if(ptr_result == NULL){
        printf("Error! Memory not allocated.");
        exit(1);
    }

    ptr_arr = (int*) calloc(vector_size, sizeof(int));
    fp2 = fopen(argv[2], "r");
		if(fp2 == NULL){
			perror("File Read Error!\nmain(): ");
			exit(1);
	}
    // copy vector file to an array
	while(fscanf(fp2, "%d%d", &row, &val) == 2){
				ptr_arr[row-1] = val;
                printf("%d ", ptr_arr[row-1]);
	}



	// contents
    printf("v: %d\n", vector_size);

    // allocate mapper thread array
	ptr_tid = (pthread_t*) calloc(num_splits, sizeof(pthread_t));
	if(ptr_tid == NULL){
		printf("Error! Memory not allocated.");
		exit(1);
	}
    for(int i = 0; i < num_splits; i++){
        pthread_create(&ptr_tid[i], NULL, mapperThread, (int *) i);
    }
    // wait for mappers
    for(int i = 0; i < num_splits; i++){
        pthread_join(ptr_tid[i], NULL);
    }

	// print
    for(int i = 0;i < num_splits; i++){
        for(int j =0; j< vector_size; j++)
        printf("%d ", ptr_result[i][j]);
    }

    // create reducer
    pthread_create(&reducer_tid, NULL, reducerThread, argv[3]);
    pthread_join(reducer_tid, NULL);
	printf("Parent process terminating.\n");
	
	return 0;



}