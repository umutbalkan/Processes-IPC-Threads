/* 	creator: S.  Umut Balkan
	date : 02/28/2020
	filename: mvp.c
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
    int row, col, val;
    int vector_size;
	int num_lines;
	int num_splits;
    int s;
    int **ptr_pipe;
	int* ptr_arr;
	int* result_arr;
	char *char_arr;
    FILE *fp,*fp2,*fp3;
	char filename[50];
	char *readbuffer;

    // checking num of arguments
	if (argc != 5){
		printf("wrong number of arguments\n");
		printf("usage: mvp <squarematrixfile.txt> <columnvector.txt> <resultfile.txt> <numberofsplits>\n");
		exit(1);
	}

	printf("MVP started.\n");
	printf("process ID: %d\n", (int) getpid());

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

	// create mvp-splitX.txt files X = [0,num_splits-1]
	rewind(fp);
	for(int i = 0; i < num_splits; i++){
		fflush(stdout);
		// create files and write in it.
		sprintf(filename, "mvp-split%d.txt", i);
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


    // allocate 2d pipes array.
    ptr_pipe = (int**) calloc(num_splits*2, sizeof(int));
    for (int i = 0; i < num_splits; i++){
        ptr_pipe[i] = (int*) calloc(2, sizeof(int));
    }
    if(ptr_pipe == NULL){
        printf("Error! Memory not allocated.");
        exit(1);
    }

	// contents
	for(int i = 0; i< num_splits; i++){
		for(int j = 0; j < 2; j++){
			printf("i=%d j=%d, val: %d ",i, j, ptr_pipe[i][j]);
		}
		printf("\n");
	}

	// create pipes
	for (int i = 0; i < num_splits; i++){
		if(pipe(ptr_pipe[i]) == -1){
			fprintf(stderr, "Pipe Failed.");
			exit(1);
		}
	}


    // allocate child processes array
	ptr_pid = (pid_t*) calloc(num_splits, sizeof(pid_t));
	if(ptr_pid == NULL){
		printf("Error! Memory not allocated.");
		exit(1);
	}

	// create child processes & set pipe read/write end
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
		if(ptr_pid[i] == 0){  // we are in child
			printf("Child %d, pid=%d\n",i, (int) getpid());

			// Child closes up read side of each pipe
			for(int j = 0; j < num_splits; j++){
				close(ptr_pipe[j][0]);
			}

			fp2 = fopen(argv[2], "r");
			if(fp2 == NULL){
				perror("File Read Error!\nmain(): ");
				exit(1);
			}

			ptr_arr = (int*) calloc(vector_size, sizeof(int));
			result_arr = (int*) calloc(vector_size, sizeof(int));
			char_arr = (char*) calloc(vector_size, sizeof(char));
			rewind(fp2);

			// copy vector file to an array
			while(fscanf(fp2, "%d%d", &row, &val) == 2){
				ptr_arr[row-1] = val;
			}

			// open mvp-splitX.txt
			sprintf(filename, "mvp-split%d.txt", i);
			fp2 = fopen(filename, "r");
			if( fp2 == NULL){
				perror("File Read Error!\nmain(): ");
				exit(1);
			}

			// create a result array from mvp-splitX.txt and vector.txt
			while(fscanf(fp2, "%d%d%d", &row, &col, &val) == 3){
				result_arr[row-1] += ptr_arr[col-1] * val;
			}

			// put the result array in the pipe
			printf("Writing  to pipe, Child%d \n", i);
			fflush(stdout);
			for( int j = 0; j < vector_size; j++){
				char_arr[j] = result_arr[j] + '0';
			}
			write(ptr_pipe[i][1], char_arr, (strlen(char_arr)+1));
			close(ptr_pipe[i][1]);

			fclose(fp2);
			free(ptr_arr);
			free(result_arr);
			free(char_arr);
			printf("child %d terminating.\n", i);
			exit(0);
		} else { // we are in parent
				// Parent process closes up writing side of each pipe
				close(ptr_pipe[i][1]);
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
        printf("processing pipes...\n");
		result_arr = (int*) calloc(vector_size, sizeof(int));

        // read from pipe
		for( int j = 0; j < num_splits; j++){

			// Read string from each pipe
			readbuffer = (char*) calloc(vector_size, sizeof(char));
			read(ptr_pipe[j][0], readbuffer, sizeof(readbuffer));

			// create result array
			for(int i = 0; i < vector_size; i++){
				result_arr[i] += readbuffer[i] - '0';
			}

			free(readbuffer);
		}

    printf("creating resultfile, in sorted order.\n");
	fp2 = fopen(argv[3],"w");
	if(fp2 == NULL){
		perror("File Read/Write Error!\nmain(): ");
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

		// free mem
		for (int i = 0;i < num_splits; i++){
			free(ptr_pipe[i]);
		}
		free(ptr_pipe);
		free(ptr_pid);
        exit(0);
    }
	return 0;



}