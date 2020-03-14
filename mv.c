/* 	creator: S.  Umut Balkan
	date : 02/22/2020
	filename: mv.c
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/wait.h>

int main(int argc, char *argv[]){

	pid_t* ptr_pid;
	pid_t reducer_pid;
	FILE *fp,*fp2;
	int row, col, val;
	int vector_size;
	int num_lines;
	int num_splits;
	char filename[50];
	int s; // used for ceiling
	int* ptr_arr;
	int* result_arr;
	// checking num of arguments
	if (argc != 5){
		printf("wrong number of arguments\n");
		printf("usage: mv <squarematrixfile.txt> <columnvector.txt> <resultfile.txt> <numberofsplits>\n");
		exit(1);
	}

	printf("MV started.\n");
	printf("process ID: %d\n", (int) getpid());

	// getting & setting arguments
	num_splits = atoi(argv[4]);
	fp = fopen(argv[1], "r");
	fp2 = fopen(argv[2], "r");

	// checking inputs
	if(fp == NULL){
		printf("file: %s", argv[1]);
		perror("main(): "); // print error on stderr
		exit(1); // EXIT_FAILURE
	}
	if(fp2 == NULL){
		printf("file: %s", argv[2]);
		perror("main(): ");
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
	printf("size of vectorfile.txt: %d\n", vector_size);

	// creating splits
	s = num_lines / num_splits;
	printf("s value: %d\n", s);
	rewind(fp);
	for(int i = 0; i < num_splits; i++){
		fflush(stdout);
		// create files and write in it.
		sprintf(filename, "split%d.txt", i);
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

	// create child processes
	ptr_pid = (pid_t*) calloc(num_splits, sizeof(pid_t));
	if(ptr_pid == NULL){
		printf("Error! Memory not allocated.");
		exit(1);
	}
	for(int i = 0; i < num_splits; i++){
		ptr_pid[i] = fork();
		if(ptr_pid[i] < 0){
			perror("process error!\nmain(): ");
			exit(1);
		}
		if(ptr_pid[i] == 0){
			printf("Child %d, pid=%d\n",i, (int) getpid());
			fp2 = fopen(argv[2], "r");
			if(fp2 == NULL){
				perror("File Read Error!\nmain(): ");
				exit(1);
			}
			ptr_arr = (int*) calloc(vector_size, sizeof(int));
			result_arr = (int*) calloc(vector_size, sizeof(int));
			rewind(fp2);
			// copy vector file to an array
			while(fscanf(fp2, "%d%d", &row, &val) == 2){
				ptr_arr[row-1] = val;
			}
			// open splitX.txt
			sprintf(filename, "split%d.txt", i);
			fp2 = fopen(filename, "r");
			if( fp2 == NULL){
				perror("File Read Error!\nmain(): ");
				exit(1);
			}
			// create a result array from splitX.txt and vector array
			while(fscanf(fp2, "%d%d%d", &row, &col, &val) == 3){
				result_arr[row-1] += ptr_arr[col-1] * val;
			}

			// create intermediateX.txt files
			fflush(stdout);
			sprintf(filename, "intermediate%d.txt",i);
			fp2 = fopen(filename, "w");
			if( fp2 == NULL){
				perror("File Read Error!\nmain(): ");
				exit(1);
			}
			for( int j = 0; j < vector_size; j++){
				fprintf(fp2, "%d %d\n", j+1, result_arr[j]);
			}

			fclose(fp2);
			free(result_arr);
			printf("child %d terminating.\n", i);
			exit(0);
		}
	}

	// wait for mapper processes to finish.
	for(int i = 0; i < num_splits; i++){
		if(ptr_pid != 0){
            printf("This is parent, pid: %d\nWaiting child %d, pid: %d\n", (int) getpid(), i, (int) ptr_pid[i]);
			waitpid(ptr_pid[i], NULL, 0);
            printf("Child %d terminated\n", i);
        }
	}

	// create reducer process
    printf("\nall mapper processes finished.\ncreating reducer process.\n");
    reducer_pid = fork();
	if(reducer_pid < 0){
		perror("process error!\nmain(): ");
		exit(1);
	}
    if(reducer_pid == 0){
        printf("\nThis is reducer, pid=%d\n", (int) getpid());
        printf("processing intermediate files...\n");
		result_arr = (int*) calloc(vector_size, sizeof(int));
        // read intermediate files
		for( int j = 0; j < num_splits; j++){
			fflush(stdout);
			sprintf(filename, "intermediate%d.txt", j);
			fp2 = fopen(filename, "r");
           		if(fp2 == NULL){
                    	perror("File Read Error!\nmain(): ");
                    	exit(1);
    	        }
			// create result array
			while(fscanf(fp2, "%d%d", &row, &val) == 2){
				result_arr[row-1] += val;
			}
		}
    printf("creating resultfile, in sorted order.\n");
	fp2 = fopen(argv[3],"w");
	if(fp2 == NULL){
		perror("File Read Error!\nmain(): ");
		exit(1);
	}
	for( int j = 0; j < vector_size; j++){
		fprintf(fp2, "%d %d\n", j+1, result_arr[j]);
	}
	fclose(fp2);
    printf("reducer terminating.\n");
	free(result_arr);
    exit(0);
    }
	else {
		printf("This is parent, waiting for reducer process exit\n");
        waitpid(reducer_pid, NULL, 0);
    	printf("Parent terminating.\n");
		fclose(fp);
		free(ptr_pid);
        exit(0);
    }
	return 0;
}
