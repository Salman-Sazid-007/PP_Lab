#include <iostream>
#include <mpi.h>
#include <string.h>
#include <sstream>
#include <fstream>
#include <vector>

using namespace std;

/* send string */
void send_string(const string &text, int receiver){
    int len = text.size() + 1;
    MPI_Send(&len, 1, MPI_INT, receiver, 1, MPI_COMM_WORLD);
    MPI_Send(text.c_str(), len, MPI_CHAR, receiver, 1, MPI_COMM_WORLD);
}

/* receive string */
string receive_string(int sender){
    int len;
    MPI_Status status;
    MPI_Recv(&len, 1, MPI_INT, sender, 1, MPI_COMM_WORLD, &status);
    char *buf = new char[len];
    MPI_Recv(buf, len, MPI_CHAR, sender, 1, MPI_COMM_WORLD, &status);
    string result(buf);
    delete[] buf;
    return result;
}

/* convert vector chunk to string */
string vector_to_string(const vector<string> &lines, int start, int end){
    string result;
    for(int i = start; i < min((int)lines.size(), end); i++){
        result += lines[i] + "\n";
    }
    return result;
}

/* string to vector */
vector<string> string_to_vector(const string &text){
    vector<string> result;
    istringstream iss(text);
    string line;
    while(getline(iss, line)){
        if(!line.empty()) result.push_back(line);
    }
    return result;
}

/* check phone number */
bool check(const string &line, const string &search){
    return (line.find(search) != string::npos);
}

/* read phonebook files */
void phonebook_read(const vector<string> &files, vector<string> &lines){
    for(const string &file : files){
        ifstream f(file);
        string line;
        while(getline(f, line)){
            if(!line.empty()) lines.push_back(line);
        }
    }
}

int main(int argc, char *argv[]){
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc < 3){
        if(rank == 0)
            cout << "Usage: mpiexec -np <p> file1.txt [file2.txt ...] phone\n";
        MPI_Finalize();
        return 0;
    }

    string search = argv[argc - 1];
    double start_time, end_time;

    if(rank == 0){
        /* root */
        vector<string> files;
        for(int i = 1; i < argc - 1; i++)
            files.push_back(argv[i]);

        vector<string> all_lines;
        phonebook_read(files, all_lines);

        int total = all_lines.size();
        int chunk = (total + size - 1) / size;

        /* send chunks + starting line number */
        for(int i = 1; i < size; i++){
            int start_line = i * chunk;
            MPI_Send(&start_line, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            string text_chunk = vector_to_string(all_lines, start_line, (i + 1) * chunk);
            send_string(text_chunk, i);
        }

        start_time = MPI_Wtime();

        string final_result;

        /* root own chunk */
        for(int i = 0; i < min(chunk, total); i++){
            if(check(all_lines[i], search)){
                final_result += to_string(i + 1) + " : " + all_lines[i] + "\n";
            }
        }

        /* receive from workers */
        for(int i = 1; i < size; i++){
            final_result += receive_string(i);
        }

        end_time = MPI_Wtime();

        ofstream out("output.txt");
        out << final_result;
        out.close();

        cout << "Search complete\n";
        cout << "Result saved in output.txt\n";
        cout << "Total execution time: " << end_time - start_time << " seconds\n";
    }
    else{
        /* worker */
        int start_line;
        MPI_Recv(&start_line, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        string text = receive_string(0);
        vector<string> local_lines = string_to_vector(text);

        start_time = MPI_Wtime();
        string local_result;

        for(int i = 0; i < local_lines.size(); i++){
            if(check(local_lines[i], search)){
                local_result += to_string(start_line + i + 1) +
                                " : " + local_lines[i] + "\n";
            }
        }
        end_time = MPI_Wtime();

        send_string(local_result, 0);
    }

    MPI_Finalize();
    return 0;
}
