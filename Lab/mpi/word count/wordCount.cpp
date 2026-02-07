#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <mpi.h>
#include <map>
#include <algorithm>
#include <cctype>

using namespace std;

#define TEXT_TAG 1
#define MAP_TAG  2

// Count alphabetic words only
map<string,int> wordCount(const string &text) {
    map<string,int> cnt;
    string word;

    for(char c : text) {
        if(isalpha(c)) {
            word += tolower(c);
        } else {
            if(!word.empty()) {
                cnt[word]++;
                word.clear();
            }
        }
    }
    if(!word.empty()) {
        cnt[word]++;
    }
    return cnt;
}

// Send string via MPI
void send_string(const string &text, int receiver, int tag) {
    int len = text.size() + 1;
    MPI_Send(&len, 1, MPI_INT, receiver, tag, MPI_COMM_WORLD);
    MPI_Send(text.c_str(), len, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
}

// Receive string via MPI
string recv_string(int sender, int tag) {
    int len;
    MPI_Recv(&len, 1, MPI_INT, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    char *buf = new char[len];
    MPI_Recv(buf, len, MPI_CHAR, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    string result(buf);
    delete[] buf;
    return result;
}

// Convert vector slice to string
string vector_to_string(const vector<string> &lines, int start, int end) {
    string result;
    for(int i = start; i < end && i < (int)lines.size(); i++) {
        result += lines[i] + "\n";
    }
    return result;
}

// Convert map to string
string map_to_string(const map<string,int> &mp) {
    string result;
    for(auto &p : mp) {
        result += p.first + " " + to_string(p.second) + "\n";
    }
    return result;
}

// Convert string to map
map<string,int> string_to_map(const string &text) {
    map<string,int> mp;
    istringstream iss(text);
    string word;
    int count;
    while(iss >> word >> count) {
        mp[word] += count;
    }
    return mp;
}

// Read phonebook file (extract names only)
void read_phonebook(const string &file, vector<string> &names) {
    ifstream f(file);
    string line;
    while(getline(f, line)) {
        int pos = line.find(",");
        if(pos != string::npos) {
            string name = line.substr(1, pos - 2);
            names.push_back(name);
        }
    }
}

int main(int argc, char *argv[]) {

    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Validate input
    if(rank == 0 && argc < 2) {
        cout << "Usage: mpirun -np <p> ./wordcount phonebook1.txt [K]\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Parse K (Top-K)
    int K = -1;   // -1 means ALL
    if(argc >= 3) {
        K = atoi(argv[2]);
        if(K <= 0) K = -1;
    }

    if(rank == 0) {
        // ===== ROOT =====
        vector<string> all_names;
        read_phonebook(argv[1], all_names);

        int total = all_names.size();
        int chunk = (total + size - 1) / size;

        // Send chunks
        for(int i = 1; i < size; i++) {
            string text = vector_to_string(all_names,
                                           i * chunk,
                                           (i + 1) * chunk);
            send_string(text, i, TEXT_TAG);
        }

        // Root chunk
        map<string,int> final_count =
            wordCount(vector_to_string(all_names, 0, chunk));

        // Receive worker results
        for(int i = 1; i < size; i++) {
            string text = recv_string(i, MAP_TAG);
            map<string,int> local_map = string_to_map(text);
            for(auto &p : local_map) {
                final_count[p.first] += p.second;
            }
        }

        // Sort
        vector<pair<string,int>> result(final_count.begin(), final_count.end());
        sort(result.begin(), result.end(),
             [](auto &a, auto &b){ return a.second > b.second; });

        // Write output
        ofstream out("wordCount.txt");

        int limit = (K == -1) ? result.size()
                              : min(K, (int)result.size());

        for(int i = 0; i < limit; i++) {
            out << result[i].first << " " << result[i].second << "\n";
        }
        out.close();

        if(K == -1)
            cout << "All word counts written to wordCount.txt\n";
        else
            cout << "Top " << K << " word counts written to wordCount.txt\n";
    }
    else {
        // ===== WORKER =====
        string local_text = recv_string(0, TEXT_TAG);
        map<string,int> local_count = wordCount(local_text);
        send_string(map_to_string(local_count), 0, MAP_TAG);
    }

    MPI_Finalize();
    return 0;
}
