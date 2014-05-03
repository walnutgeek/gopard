/*
 ============================================================================
 Name        : gopard.c
 Author      :
 Version     :

 Copyright   : Apache License

 Description :

 Java notoriously bad with forking processes, because
  jvm usually have large memory footprint, and fork copy all that memory twice
  make it very inefficient .

 gopard is small executor. Intention is to use it with java/scala to
 execute jobs, but there no assumption in executor itself
 that control process will be jvm. gopard operations controled very simple
 protocol.

 Control program invokes processes by printing commands into standard output

 Execute process - exec:command line

 Control program can also listen on standard input about program invocations
 events:

 invoked.csv
 id,pid,runType,startTime,statusDirectory,cmd

 finished.csv
 id,pid,runType,returnCode,startTime,endTime,duration,statusDirectory,cmd


 gopard will exit when control process and all spawned processes are finished.

 gopard.jar is library that help provide java api to take care of all interactions
 with executor from java.

 ============================================================================
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
#include <sys/stat.h>

#define BUFF_SIZE 1024
static char buff[BUFF_SIZE];
static char statusRoot[512];
static char controlPath[512];


#define TIMESTAMP_TEMPLATE   "%04d-%02d-%02d %02d:%02d.%02d"
#define TIMESTAMP_EXTRACT(t) t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec

#define ID_TEMPLATE       "d%04d%02d%02dt%02d%02d%02d"
#define ID_EXTRACT(t)   t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec


void mkdirs(const char *dir) {
	struct stat s;
	int err = stat(dir, &s);
	if(-1 != err && S_ISDIR(s.st_mode)) {
		return;
	}
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp),"%s",dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/')
		tmp[len - 1] = 0;
	for(p = tmp + 1; *p; p++){
		if(*p == '/') {
			*p = 0;
			mkdir(tmp, 0755);
			*p = '/';
		}
	}
	mkdir(tmp, 0755);
}

int zapNextChar(char * b, int sz, char z){
	for (int i = 0; i < sz; ++i) {
		if( b[i] == z ){
			b[i] = 0;
			return i + 1;
		}
	}
	return -1;
}

int zapAll(char * b, int sz, char z){
	int c = 0;
	for( int p = 0 ; -1 != (p = zapNextChar(b+p,sz-p,z)) ; c++ );
	return c;
}

char ** extractStrings(char * b, int s){
	char ** strings;
	int count = 0;
	for (int pass = 0; pass < 2; ++pass) {
		if(pass == 1){
			strings = malloc(sizeof(char*)*(count+1));
		}
		count = 0;
		bool wasZero = true;
		for (int i = 0; i < s; ++i) {
			if( b[i] ){
				if( wasZero ) {
					if( pass == 1)
						strings[count] = b + i;
					count += 1;
				}
				wasZero = false;
			}else{
				wasZero = true;
			}
		}
	}
	strings[count] = NULL;
	return strings;
}


typedef struct {
	char * head ;
	int size;
	int used;
} Buff;

void _buff_allocate(Buff *buff, int sz) {
	buff->head = malloc(sz);
	buff->size = sz;
	buff->used=0;
}

void _buff_reset(Buff* buff){
	buff->used = 0;
}
void _buff_free(Buff* buff){
	free(buff->head);
	buff->used = buff->size = 0;
	buff->head = NULL;
}

#define _buff_tail(b) ((b)->head+(b)->used)
#define _buff_left(b) ((b)->size-(b)->used)

void _buff_processLines(Buff *buff, void (*callback)(char *) ){
	int next;
	int p = 0 ;
	while( -1 != (next = zapNextChar(buff->head+p,buff->used-p,'\n'))   ){
		(*callback)(buff->head+p);
		p = next;
	}
	memmove(buff->head, buff->head+p, buff->used-p);
	buff->used -= p;
}

char* toId(time_t tt, pid_t pid){
	struct tm  *t = localtime(&tt);
	char *p = malloc(64);
	snprintf(p, 32, ID_TEMPLATE "p%d", ID_EXTRACT(t), pid);
	return p;
}


#define GENERATE_ENUM(ENUM) ENUM,
#define GENERATE_STRING(STRING) #STRING,

#define RUN_TYPES(M) \
        M(CONTROL)   \
        M(RUNNING)  \
        M(DONE)   \
	    M(DEFAULT)

typedef enum {
    RUN_TYPES(GENERATE_ENUM)
} RunType;
static const char * runTypeNames[] = {
    RUN_TYPES(GENERATE_STRING)
};

static Buff controlBuffer;
static Buff inputBuffer;

typedef struct {
	bool stored;
	size_t size;
	time_t time;
} PipeEvent;

void _event_set(PipeEvent * event, size_t size){
	event->stored = 0;
	event->size = size;
	event->time = time(0);
}

void _event_set_iftime(PipeEvent * event, size_t size){
	if( event->size < size && event->time < (time(0) - 9) ){
		_event_set(event, size);
	}
}

typedef struct  {
	int in;
	int out;
	size_t counter;
	PipeEvent event;
	char* name;
} FilePipe;

void _pipe_init(FilePipe * pipe, char * name){
	pipe->counter = 0;
	_event_set(&(pipe->event),0);
	pipe->in = -1;
	pipe->out = -1;
	pipe->name = name;
}

size_t _pipe_copy(FilePipe * pipe, fd_set * set, Buff* buff, void (*callback)(Buff*)){
	ssize_t cnt = 0 ;
	if( FD_ISSET(pipe->in,set) ){
		char *tail = _buff_tail(buff);
		cnt = read(pipe->in,tail,_buff_left(buff));
		if(cnt<0){
			if( errno != EAGAIN) {
				fprintf(stderr, "copy: read failed: errno=%d\n", errno);
			}
		}else if(cnt>0) {
			_event_set_iftime(&(pipe->event),pipe->counter);
			write(pipe->out,tail,cnt);
			pipe->counter += cnt;
			buff->used +=cnt;
			if(callback) (*callback)(buff);
		}
		//printf("copy fd(%d) count=%ld\n",pipe->in,cnt);
	}
	return cnt;
}

typedef enum {
	DIRECTORY,
	OUT_FILE,
	ERR_FILE,
	INDEX_FILE,
} PathType;

static char *pathSuffix[] = {
		"",
		"/stdout.log",
		"/stderr.log",
		"/stdindex.csv"
};

typedef struct  {
	char *id;
	RunType runType;
	pid_t pid;
	FilePipe std_out;
	FilePipe std_err;
	FILE * index;
	int control_in ;
} Run;

#define MAX_RUN FD_SETSIZE/2
static int maxRun=MAX_RUN;
static Run* runs[MAX_RUN];

void _runs_init(){
	for (int i=0; i<maxRun; ++i) {
    	runs[i] = (Run*)0;
	}
}

Run* _runs_add(RunType type, char *id ){
	int runIdx=0;
	for(;;){
		if(!runs[runIdx]) break;
		runIdx += 1;
		if( runIdx >= maxRun) return NULL;
	}
	Run * run = malloc(sizeof(Run));
	runs[runIdx] = run;
	run -> runType = type;
	run -> id = id;
	_pipe_init(&(run->std_out),"out");
	_pipe_init(&(run->std_err),"err");
	run -> index = NULL;
	run -> control_in = -1;
	return run;
}
char* _run_path(Run * run , RunType rt, PathType pt){
	if(rt == DEFAULT){
		rt = run->runType ;
	}
	snprintf(buff,sizeof(buff),"%s/%s/%s%s",statusRoot, runTypeNames[rt], run->id, pathSuffix[pt] );
	return buff;
}

Run* _run_open(Run* run, int inputStdOut, int inputStdErr){
	run->std_out.in = inputStdOut;
	run->std_err.in = inputStdErr;
//	printf("open err=%d, out=%d\n", run->std_err.in, run->std_out.in );
	mkdirs(_run_path(run,DEFAULT,DIRECTORY));
	run->std_out.out = open(_run_path(run, DEFAULT, OUT_FILE), O_WRONLY|O_CREAT , 0644);
	run->std_err.out = open(_run_path(run, DEFAULT, ERR_FILE), O_WRONLY|O_CREAT , 0644);
	run->index = fopen(_run_path(run, DEFAULT, INDEX_FILE), "w");
	fprintf(run->index,"stream,time,size\n");
	return run;
}

void _run_storePipeEvent(Run * run, FilePipe * pipe) {
	if (!pipe->event.stored) {
		struct tm * t = localtime(&(pipe->event.time));
		fprintf(run->index, "%s,"         TIMESTAMP_TEMPLATE  ",%ld\n",
				             pipe->name,  TIMESTAMP_EXTRACT(t), pipe->event.size);
		pipe->event.stored = true;
	}
}

int _runs_prepareDescriptors(fd_set * readSet){
	int maxfd = 0;
	FD_ZERO(readSet);
	for (int runIdx = 0; runIdx < maxRun && runs[runIdx]; ++runIdx) {
		Run* run =runs[runIdx];
		if(maxfd < run->std_err.in){
			maxfd = run->std_err.in;
		}
		if(maxfd < run->std_out.in){
			maxfd = run->std_out.in;
		}
		_run_storePipeEvent(run,&(run->std_out));
		_run_storePipeEvent(run,&(run->std_err));
		FD_SET(run->std_out.in,readSet);
		FD_SET(run->std_err.in,readSet);
	}
	return maxfd+1;
}


void _pipe_free(FilePipe* pipe){
	close(pipe->in);
	close(pipe->out);
}

void _run_free(Run* run){
	if(run->control_in > -1) close(run->control_in);
	_event_set(&(run->std_out.event),run->std_out.counter);
	_event_set(&(run->std_err.event),run->std_err.counter);
	_run_storePipeEvent(run,&(run->std_out));
	_run_storePipeEvent(run,&(run->std_err));
	_pipe_free(&(run->std_err));
	_pipe_free(&(run->std_out));
	fclose(run->index);
	free(run->id);
	free(run);
}

Run * _run_new(char ** cmd,RunType runType){
	int  runPipes[6];
	pipe(runPipes);
	pipe(runPipes+2);
	if(runType==CONTROL)
		pipe(runPipes+4);
	pid_t pid;
	Run * run;
	if((pid = fork()) == -1){
		perror("fork");
		exit(1);
	}
	time_t tt = time(0);
	if( pid == 0 ) {
		close(runPipes[0]);
		dup2(runPipes[1], STDOUT_FILENO);
		close(runPipes[1]);
		close(runPipes[2]);
		dup2(runPipes[3], STDERR_FILENO);
		close(runPipes[3]);
		if(runType==CONTROL){
		    dup2(runPipes[4], STDIN_FILENO);
		    close(runPipes[4]);
			close(runPipes[5]);
		}
		char *path = _run_path(run = _runs_add(runType, toId(tt,pid=getpid())),DEFAULT,DIRECTORY);
		mkdirs(path);
		chdir(path);
		execve(cmd[0],cmd,NULL);
		fprintf(stderr, "failed to execute errno(%d): %s ...\n", errno, cmd[0]);
		exit(-1);
	}else{
		run = _runs_add(runType, toId(tt,pid));
		run->pid = pid;
		close(runPipes[1]);
		close(runPipes[3]);
		if(runType==CONTROL){
			close(runPipes[4]);
			run->control_in = runPipes[5];
		}
		_run_open(run,runPipes[0], runPipes[2]);
	}
	return run;
}


static Run* ctrlRun;

void _processControlCommand(char * cmd){
	int sz = strlen(cmd);
	int p = zapNextChar(cmd,sz,':');
	if(p == -1){
		fprintf(stderr,"Unrecognized command=%s\n",cmd);
	}else{
		if(strcmp(cmd,"exec")==0){
			zapAll(cmd+p,sz-p,' ');
			char ** execStrings = extractStrings(cmd+p,sz-p);
			_run_new(execStrings,RUNNING);
			free(execStrings);
		}else if(strcmp(cmd,"print")==0){
			puts(cmd+p);
		}else{
			fprintf(stderr,"Unknown command=%s:%s\n",cmd,cmd+p);
		}
	}

}

void _process_control_output(Buff* buff){
	_buff_processLines(buff,&_processControlCommand);
}


void _runs_processOutput(fd_set * set){
	for (int runIdx = 0; runIdx < maxRun && runs[runIdx]; ++runIdx) {
		Run* run =runs[runIdx];
		if(run->runType == CONTROL){
			_pipe_copy( &(run->std_out),set, &controlBuffer, &_process_control_output  );
		}else{
			_pipe_copy( &(run->std_out),set, &inputBuffer, &_buff_reset);
		}
		_pipe_copy( &(run->std_err),set, &inputBuffer, &_buff_reset);
	}
}

int _runs_checkExits(){
	int status;
	pid_t pid ;
	while((pid =waitpid(-1,&status, WNOHANG)) > 0 ){
		int collapse = 0 ;
		for (int runIdx = 0; runIdx < maxRun && runs[runIdx]; ++runIdx) {
			Run* run = runs[runIdx];
			if( run->pid ==  pid ){
				_run_free(run);
				collapse = 1;
			}
			if(collapse){
				runs[runIdx] = runs[runIdx+1];
			}
		}
	}
	return runs[0] != NULL;
}


int main(int argc, char **argv) {
    if ( argc < 3 ){
    	printf("USAGE: gopard <output directory> <control process command and arguments> \n");
    	return EXIT_FAILURE;
    }
    _runs_init();
    realpath(argv[1],statusRoot);
    realpath(argv[2],controlPath);
    int nArgs = argc-2;
    char ** cmd = malloc( sizeof(char*) * nArgs );
    _buff_allocate(&inputBuffer, 0x8000);
    _buff_allocate(&controlBuffer, 0x2000);
    cmd[0]=controlPath;
    for (int iCmd = 1; iCmd < nArgs; ++iCmd) {
    	cmd[iCmd] = argv[2+iCmd];
	}
    ctrlRun = _run_new(cmd,CONTROL);
	struct timeval timeout;
	do{
		timeout.tv_sec  = 10;
		timeout.tv_usec = 0;
		fd_set         input;
		int n = select(_runs_prepareDescriptors(&input), &input, NULL, NULL, &timeout);
		/* See if there was an error */
		if (n < 0){
			perror("select failed");
		}else if (n){
			_runs_processOutput(&input);
		}
	}while(_runs_checkExits());
    free(cmd);
    _buff_free(&inputBuffer);
    _buff_free(&controlBuffer);
}


