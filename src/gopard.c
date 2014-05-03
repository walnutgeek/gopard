/*
 ============================================================================
 Name        : gopard.c
 Author      : 
 Version     :

 Copyright   : Apache License

 Description :

 Java notoriously bad with forking processes, because
  jvm usually have large memory footprint, and fork may copy all that memory twice
  make it very inefficient.
  Another problem gopard solves is IO, to save standard err/out you need monitor streams
  in separate thread. gopard use non-blocking IO to capture streams and will handle
  it in one thread.

 gopard is small executor. Intention is to use it with java/scala to
 execute jobs, but there no assumption in executor code
 that control process will be jvm. gopard operations controlled by very simple
 protocol.

 Control program invokes processes by printing commands into standard output

 Execute process - exec:<command line>
 Print something - print:<text>

 Control program can also listen on standard input about program invocations
 events:

 invoked.csv
 id,pid,runType,startTime,statusDirectory,cmd

 running.csv
 id,pid,runType,startTime,statusDirectory,duration,cmd

 finished.csv
 id,pid,runType,returnCode,startTime,endTime,duration,statusDirectory,cmd


 gopard will exit when control process and all spawned processes are finished.

 I have intention to create gopard.jar. Java/Scala api to take
 care of all interactions with executor.

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
#define TIMESTAMP_EXTRACT(t) (t)->tm_year + 1900, (t)->tm_mon + 1, (t)->tm_mday, (t)->tm_hour, (t)->tm_min, (t)->tm_sec

#define ID_TEMPLATE     "d%04d%02d%02dt%02d%02d%02d"
#define ID_EXTRACT(t)   (t)->tm_year + 1900, (t)->tm_mon + 1, (t)->tm_mday, (t)->tm_hour, (t)->tm_min, (t)->tm_sec


void mkdirs(const char *dir, bool onlyEnsureParent) {
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
	if(!onlyEnsureParent) mkdir(tmp, 0755);
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
	int c = 0, p = 0, offset;
	while( -1 != (offset = zapNextChar(b+p,sz-p,z)) ){
		c += 1 ;
		p += offset;
	}
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
		p += next;
	}
	buff->used -= p;
	memmove(buff->head, buff->head+p, buff->used);
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

void _pipe_free(FilePipe* pipe){
	close(pipe->in);
	close(pipe->out);
}

size_t _pipe_copy(FilePipe * pipe, fd_set * set, Buff* buff, void (*callback)(Buff*)){
	ssize_t cnt = 0 ;
	if( FD_ISSET(pipe->in,set) ){
		char *tail = _buff_tail(buff);
		cnt = read(pipe->in,tail,_buff_left(buff));
		if(cnt<0){
			if( errno != EAGAIN) {
				fprintf(stderr, "copy: read failed: errno=%s(%d)\n", strerror(errno),errno);
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
	RUNNING_FILE,
	INVOKED_FILE,
	FINISHED_FILE,
} PathType;

static char *pathSuffix[] = {
		"",
		"/stdout.log",
		"/stderr.log",
		"/stdindex.csv",
		"/running.csv",
		"/invoked.csv",
		"/finished.csv",
};


typedef struct  {
	char *id;
	RunType runType;
	pid_t pid;
	FilePipe std_out;
	FilePipe std_err;
	FILE * index;
	int control_in ;
	time_t start;
	time_t end;
	int returnCode;
	char * cmd ;
} Run;

char* _run_path(Run * run , RunType rt, PathType pt){
	if(rt == DEFAULT){
		rt = run->runType ;
	}
	snprintf(buff,sizeof(buff),"%s/%s/%s%s",statusRoot, runTypeNames[rt], run->id, pathSuffix[pt] );
	return buff;
}

#define MAX_RUN FD_SETSIZE/2
static int maxRun=MAX_RUN;
static Run* runs[MAX_RUN];
static Run* ctrlRun;

static FILE * invoked ;
static FILE * finished ;



void _runs_init(){
	for (int i=0; i<maxRun; ++i) {
    	runs[i] = (Run*)0;
	}
}

static char* toId(time_t tt, pid_t pid){
	struct tm  *t = localtime(&tt);
	char *p = malloc(64);
	snprintf(p, 64, ID_TEMPLATE "p%d", ID_EXTRACT(t), pid);
	return p;
}

static char* toCmd(char ** cmdArray){
	buff[0] = 0 ;
	while(*cmdArray){
		strncat(buff, *cmdArray, sizeof(buff) - strlen(buff) - 1);
		strncat(buff, " ", sizeof(buff) - strlen(buff) - 1);
		cmdArray++;
	}
	return strdup(buff);
}

Run* _runs_add(RunType type, time_t tt, pid_t pid, char ** cmdArray ){
	int runIdx=0;
	for(;;){
		if(!runs[runIdx]) break;
		runIdx += 1;
		if( runIdx >= maxRun) return NULL;
	}
	Run * run = malloc(sizeof(Run));
	runs[runIdx] = run;
	run -> runType = type;
	run -> id = toId(tt,pid);
	run -> pid = pid ;
	_pipe_init(&(run->std_out),"out");
	_pipe_init(&(run->std_err),"err");
	run -> index = NULL;
	run -> start = tt;
	run -> end = 0;
	run -> control_in = -1;
	run -> cmd = toCmd(cmdArray);
	return run;
}


char* _run_mkdir(Run* run){
	char *path = _run_path(run,DEFAULT,DIRECTORY);
	mkdirs(path,false);
	return path;
}


Run* _run_open(Run* run, int inputStdOut, int inputStdErr){
	run->std_out.in = inputStdOut;
	run->std_err.in = inputStdErr;
	_run_mkdir(run);
//	printf("open err=%d, out=%d\n", run->std_err.in, run->std_out.in );
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


void _run_free(Run* run){
	if(run->control_in > -1) close(run->control_in);
	_event_set(&(run->std_out.event),run->std_out.counter);
	_event_set(&(run->std_err.event),run->std_err.counter);
	_run_storePipeEvent(run,&(run->std_out));
	_run_storePipeEvent(run,&(run->std_err));
	_pipe_free(&(run->std_err));
	_pipe_free(&(run->std_out));

    //TODO move to separate method
	//id,pid,runType,returnCode,startTime,endTime,duration,statusDirectory,cmd
	char * finalPath ;
	if(run->runType == CONTROL){
		finalPath = _run_path(run,CONTROL,DIRECTORY);
	}else{
		char* moveFrom = strdup(_run_path(run,DEFAULT,DIRECTORY));
		finalPath = _run_path(run,DONE,DIRECTORY);
		mkdirs(finalPath,true);
		if( -1 == rename(moveFrom,finalPath) ){
			fprintf(stderr,"rename %s -> %s failed. errno:%s(%d) \n", moveFrom, finalPath, strerror(errno),errno);
			finalPath = _run_path(run,DEFAULT,DIRECTORY);
		}
		free(moveFrom);
	}
	struct tm start = *localtime(&(run->start));
	struct tm end = *localtime(&(run->end));
	fprintf(finished,"%s,%d,%s,%d," TIMESTAMP_TEMPLATE "," TIMESTAMP_TEMPLATE ",%ld,%s,%s\n",
			run->id,
			run->pid,
			runTypeNames[run->runType],
			run->returnCode,
			TIMESTAMP_EXTRACT(&start),
			TIMESTAMP_EXTRACT(&end),
			run->end-run->start,
			finalPath,
			run->cmd
	);

	fclose(run->index);
	free(run->cmd);
	free(run->id);
	free(run);
}

static void _ctrlRun_init(Run* run){
	ctrlRun = run;
	_run_mkdir(run);
	invoked = fopen(_run_path(ctrlRun,DEFAULT,INVOKED_FILE),"w");
	fprintf(invoked, "id,pid,runType,startTime,statusDirectory,cmd\n");
    finished = fopen(_run_path(ctrlRun,DEFAULT,FINISHED_FILE),"w");
    fprintf(finished, "id,pid,runType,returnCode,startTime,endTime,duration,statusDirectory,cmd\n");

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

void _runs_updateRunning(){
	FILE * running =  fopen(_run_path(ctrlRun,DEFAULT,RUNNING_FILE),"w");
	fprintf(running, "id,pid,runType,startTime,duration,statusDirectory,cmd\n");
	for (int runIdx = 0; runIdx < maxRun && runs[runIdx]; ++runIdx) {
		Run* run =runs[runIdx];
		struct tm *t = localtime(&(run->start));
		fprintf(running, "%s,%d,%s," TIMESTAMP_TEMPLATE ",%ld,%s,%s\n",
				run->id, run->pid, runTypeNames[run->runType],
				TIMESTAMP_EXTRACT(t),time(0)-run->start,
				_run_path(run,DEFAULT,DIRECTORY),run->cmd  );
	}
	fclose(running);
}


Run * _run_new(char ** cmd,RunType runType){
	int  runPipes[6];
	pipe(runPipes);
	pipe(runPipes+2);
	if(runType==CONTROL)
		pipe(runPipes+4);
	pid_t pid;
	Run * run;
	time_t tt = time(0);
	if((pid = fork()) == -1){
		perror("fork");
		exit(1);
	}
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
		run = _runs_add(runType, tt, pid=getpid(),cmd);
		chdir(_run_mkdir(run));
		execve(cmd[0],cmd,NULL);
		fprintf(stderr, "failed to execute errno:%s(%d) cmd:%s\n", strerror(errno),errno, run->cmd);
		exit(-1);
	}else{
		run = _runs_add(runType,tt,pid,cmd);
		close(runPipes[1]);
		close(runPipes[3]);
		if(runType==CONTROL){
			close(runPipes[4]);
			run->control_in = runPipes[5];
			_ctrlRun_init(run);
		}
		_run_open(run,runPipes[0], runPipes[2]);
	}
	//TODO move to separate method
	//id,pid,runType,startTime,statusDirectory,cmd
	struct tm *start  = localtime(&(run->start));
	fprintf(invoked,"%s,%d,%s," TIMESTAMP_TEMPLATE ",%s,%s\n",
			run->id,
			run->pid,
			runTypeNames[run->runType],
			TIMESTAMP_EXTRACT(start),
			_run_path(run,DEFAULT,DIRECTORY),
			run->cmd
	);
	_runs_updateRunning();
	return run;
}

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

int _runs_checkForTerminatedJobs(){
	int status;
	pid_t pid ;
	bool changes = false;
	while((pid =waitpid(-1,&status, WNOHANG)) > 0 ){
		bool collapse = false ;
		changes = true;
		for (int runIdx = 0; runIdx < maxRun && runs[runIdx]; ++runIdx) {
			Run* run = runs[runIdx];
			if( run->pid ==  pid ){
				run->returnCode = status;
				run->end = time(0);
				_run_free(run);
				collapse = true;
			}
			if(collapse){
				runs[runIdx] = runs[runIdx+1];
			}
		}
	}
	if(changes==true){
		_runs_updateRunning();
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
    _buff_allocate(&inputBuffer, 0x8000); // 32k
    _buff_allocate(&controlBuffer, 0x2000); // 8k
    cmd[0]=controlPath;
    for (int iCmd = 1; iCmd < nArgs; ++iCmd) {
    	cmd[iCmd] = argv[2+iCmd];
	}
    _run_new(cmd,CONTROL);
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
	}while(_runs_checkForTerminatedJobs());
    free(cmd);
    fclose(invoked);
    fclose(finished);
    _buff_free(&inputBuffer);
    _buff_free(&controlBuffer);
}


