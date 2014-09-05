#include <mpi.h>
#include <fstream>
#include <iostream>
#include <cmath>
#include <string>
#include <cstdlib>
#include <vector>
#include <stdlib.h>
#include <sstream> 

using namespace std;


int hash_code (int key, int processes) {
    int result = key%(processes-1);
    return (result+1);
}

int nextIndex(int numtasks, int circularIndex) {
    circularIndex++;
    if (circularIndex == numtasks) 
        return 1;

    return circularIndex;
}

void receive_key_value(int world_rank) {


    int numtasks;
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

    map <int, map <int, int> > localDb;

    MPI_Status Stat;

    int size;

    MPI_Probe(0, 0, MPI_COMM_WORLD, &Stat);
    MPI_Get_count(&Stat, MPI_INT, &size);

    int inmsg[size];
    MPI_Recv(&inmsg, size, MPI_INT, 0, 0, MPI_COMM_WORLD, &Stat);

    for (int i = 0; i < size; i = i+2) {

        map <int, int> temp;
        int key = inmsg[i];
        int val = inmsg[i+1];

        if (localDb.find(hash_code(key, numtasks)) != localDb.end()) {
            temp = localDb[hash_code(key, numtasks)];
        }
   
        if (temp.find(key) != temp.end()) {
            int curVal = temp[key];
            val += curVal;
        }

        temp[key] = val;
        localDb[hash_code(key, numtasks)] = temp;
    }

    MPI_Request reqsSent[numtasks];
    MPI_Status sentStats[numtasks];
    int *send_dummy = (int*) malloc(sizeof(int));
    *send_dummy=-1;
    int sent_index = 0;
    for (int i = 1; i < numtasks; i++) {
        int pid = i;
        if (world_rank == pid) 
            continue;
        
        if (localDb.find(i) == localDb.end()) {
            MPI_Isend(send_dummy, 1, MPI_INT, pid, 1, MPI_COMM_WORLD, &reqsSent[pid]);
            continue;
        }

        map <int, int> val = localDb[i];
        int *send_buffer = (int*) malloc(2*val.size()*sizeof(int));
   
        map <int, int>::iterator local_iter;
        int index = 0;
        for (local_iter = val.begin(); local_iter != val.end(); ++ local_iter) {
            send_buffer[index++] = local_iter->first;
            send_buffer[index++] = local_iter->second;
        }
        if (world_rank != pid) {
            MPI_Isend(send_buffer, index, MPI_INT, pid, 1, MPI_COMM_WORLD, &reqsSent[pid]);
            localDb.erase(pid);
        }
    }

    map <int, int> result;
    result = localDb[world_rank];

    map <int, int*> recvd_buffers;
    map <int, int > recvd_size;
    MPI_Status Stats[numtasks];

    int rcvd_count = 0;
    int flag[numtasks];
    int stateFlag[numtasks];
    MPI_Request reqsRecvd[numtasks];
    int countFlags = 0;
    int circularIndex = 1;

    for (int i = 0; i < numtasks; i++) {
        flag[i] = 0;
        stateFlag[i] = 0;
    }

    flag[world_rank] = 1;stateFlag[world_rank] = 1;
    flag[0] = 1;stateFlag[0] = 1;

    while (countFlags < numtasks-2) {
        
        if (circularIndex == world_rank) {
            circularIndex = nextIndex(numtasks, circularIndex);
            continue;
        }
        if (stateFlag[circularIndex] == 0)
            MPI_Iprobe(circularIndex, 1, MPI_COMM_WORLD, &flag[circularIndex], &Stats[circularIndex]);
        
        if (stateFlag[circularIndex] != flag[circularIndex]) {
            if (flag[circularIndex]) {
                countFlags++;
                int sizeR;
                MPI_Get_count(&Stats[circularIndex], MPI_INT, &sizeR);
                stateFlag[circularIndex] = 1;

                int* msg = (int*) malloc (sizeof(int)*sizeR);
                recvd_buffers[circularIndex] = msg;
                recvd_size[circularIndex] = sizeR;

                MPI_Irecv(msg, sizeR, MPI_INT, circularIndex, 1, MPI_COMM_WORLD, &reqsRecvd[circularIndex]);
            }
        }
        circularIndex = nextIndex(numtasks, circularIndex);
    }
    
    int doneCount = 0;
    circularIndex = 1;
    int flagTrackSent[numtasks];
    int flagTrackRecvd[numtasks];


    for (int i = 0; i < numtasks; i++) {
        flagTrackSent[i] = 0;
        flagTrackRecvd[i] = 0;
    }

    flagTrackSent[0] = 1; flagTrackSent[world_rank] = 1;
    flagTrackRecvd[0] = 1; flagTrackRecvd[world_rank] = 1;

    while (doneCount < 2*(numtasks-2)) {
        
        if (circularIndex == world_rank) {
            circularIndex = nextIndex (numtasks, circularIndex);
            continue;
        }

        if (flagTrackSent[circularIndex] == 0) {
            MPI_Test(&reqsSent[circularIndex], &flagTrackSent[circularIndex],MPI_STATUS_IGNORE);
            if (flagTrackSent[circularIndex] == 1) {
                doneCount++;
            }
        }

        if (flagTrackRecvd[circularIndex] == 0) {
            MPI_Test(&reqsRecvd[circularIndex], &flagTrackRecvd[circularIndex],MPI_STATUS_IGNORE);
            if (flagTrackRecvd[circularIndex] == 1) {
                int *completeMsg = recvd_buffers[circularIndex];
                int sizeR = recvd_size[circularIndex];
                if (sizeR != 1) {
                    for (int i = 0; i < sizeR; i=i+2) {
                        int key = completeMsg[i];
                        int value = completeMsg[i+1];
                        result[key] += value;
                    }
                }
                doneCount++;
            }
        }
        circularIndex = nextIndex (numtasks, circularIndex);
    }

    string fileName("result");
    stringstream ss;
    ss << world_rank;
    string str = ss.str();
    fileName.append(str);
    ofstream myfile (fileName.c_str());
    map <int, int>::iterator iterFile;
    int count = 0;
    if (result.size() == 0) {
        int sendBack = -1;
        MPI_Send(&sendBack, 1, MPI_INT, 0, 3, MPI_COMM_WORLD);
        return;
    }
    
    int sendBack[result.size()*2];
    for (iterFile = result.begin(); iterFile != result.end(); ++iterFile) {
        //if (iterFile->first == 4926) 
        sendBack[count++] = iterFile->first;
        sendBack[count++] = iterFile->second;

    }

    MPI_Send(&sendBack, count, MPI_INT, 0, 3, MPI_COMM_WORLD);
    
}

void parse_and_distribute(const char* filename, const int processes) {

    if (processes == 0) 
        return;

    ifstream reader(filename);
    int dest = 1;
    int numPairs = 0;

    if (reader.is_open()) {
        string line;
        getline(reader, line);
        while(getline(reader, line)) {
            numPairs++;
        }
    }

    reader.clear();
    reader.seekg(0,ios::beg);

    int chunkSize = ceil((float)numPairs/processes);
    if (reader.is_open()) {
        string line;
        int data[2*chunkSize];
        int count = 0;
        getline(reader, line);
        while(getline(reader, line)) {
            char *cstr = const_cast<char*>(line.c_str());
            int a = strtol(cstr,&cstr,10);
            cstr++;
            int b = strtol(cstr,&cstr,10);
            data[count++] = a;
            data[count++] = b;

            if (count >= 2*chunkSize) {
                MPI_Send(&data, count, MPI_INT, dest++, 0, MPI_COMM_WORLD);
                count = 0;
            }
        }
        if (count > 0) {
            MPI_Send(&data, count, MPI_INT, dest++, 0, MPI_COMM_WORLD);
        }
    }
    for (int i = dest; i <= processes; i++) {
        int data = -1;
        MPI_Send(&data, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
    }
}

int main(int argc, char** argv) {
    
    //Initialize the MPI environment
    MPI_Init(NULL, NULL);
    
    // Get the number of processes
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    //Get the rank of the process
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    
    // Get the name of the processor
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);
 
    map <int, int> kv;

    if (world_rank == 0) {

        //parse_and_distribute("1k", world_size-1);
        parse_and_distribute("100000_key-value_pairs.csv", world_size-1);
        int countRecvd = 1;
        while (countRecvd < world_size) {
            MPI_Status Stat;
            int size;
            MPI_Probe(countRecvd, 3, MPI_COMM_WORLD, &Stat);
            MPI_Get_count(&Stat, MPI_INT, &size);
            
            if (size == 1) {
                countRecvd++;
                continue;
            }

            int msg[size];

            MPI_Recv(&msg, size, MPI_INT, countRecvd, 3, MPI_COMM_WORLD, &Stat);
            
            for (int i = 0; i < size; i = i + 2) {
                int key = msg[i];
                int value = msg[i+1];
                kv[key] = value;
            }
            countRecvd++;
        }
        map <int, int>::iterator iter;

        string fileName("Output_Task2.txt");
        ofstream myfile (fileName.c_str());
        for (iter = kv.begin(); iter != kv.end(); ++iter) {
            myfile << iter->first << " " << iter->second << endl;
        }
    }
    else
        receive_key_value(world_rank);
    MPI_Finalize();

    return 0;
}
