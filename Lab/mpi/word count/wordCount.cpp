#include<vector>
#include<iostream>
#include<sstream>
#include<fstream>
#include<mpi.h>
#include<map>
#include<algorithm>
using namespace std;

map<string,int> wordCount(const string text){
    map<string,int> cnt;
    string clean;
    for( char c :text){
        if(isalnum(c)){
            clean += tolower(c);
        }
        else{
            if(!clean.empty()){
                cnt[clean]++;
                clean.clear();
            }
        }
    }
     if(!clean.empty()){
                cnt[clean]++;
                clean.clear();
            }
        return cnt;
}
void send(const string &text,int receiver){
    int len = text.size()+1;
    MPI_Send(&len,1,MPI_INT,receiver,1,MPI_COMM_WORLD);
    MPI_Send(text.c_str(),len,MPI_CHAR,receiver,1,MPI_COMM_WORLD);
}
string receive(int sender){
    int len;
    MPI_Recv(&len,1,MPI_INT,sender,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    char *buf =new char[len];
    MPI_Recv(buf,len,MPI_CHAR,sender,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    string res(buf);
    delete[] buf;
    return res;
}
string vector_to_string(vector<string> &lines,int start,int end){
    int total = lines.size();
    string result;
    for(int i=start;i<min(total,end);i++){
        result += lines[i]+"\n";
    }
    return result; 
}
string map_to_string(map<string,int> mp){
    string result;
    for(auto &p:mp){
        result += p.first + " " + to_string(p.second) +"\n";
    }
    return result;
}
map<string,int> string_to_map(const string &text){
    map<string,int> result;
    istringstream iss(text);
    string word;
    int count;
    while(iss >> word >>count){
        result[word]+=count;
    }
    return result;
}

void read_file(const string file,vector<string> &lines){
    ifstream f(file);
    string line;
    while(getline(f,line)){
        if(!line.empty()) lines.push_back(line);
    }
}
int main(int argc ,char *argv[]){
    MPI_Init(&argc,&argv);
    int rank,size;
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    if(rank==0){
      if(argc<2){
            cout<<"<filename>\n";
            MPI_Finalize();
            return 1;
        }
    }
    if(rank == 0){
        vector<string> all_lines;
        read_file(argv[1],all_lines);
        int total = all_lines.size();
        int chunk = (total+size-1)/size;
        for(int i=1;i<size;i++){
            string text = vector_to_string(all_lines,i*chunk,(i+1)*chunk);
            send(text,i);
        }
        map<string,int> final_count = wordCount(vector_to_string(all_lines,0,chunk));
        for(int i=1;i<size;i++){
            string text = receive(i);
            map<string,int> tmp = string_to_map(text);
            for(auto &p:tmp){
                final_count[p.first]+=p.second;
            }
        }
        vector<pair<string,int>> v(final_count.begin(),final_count.end());
        sort(v.begin(),v.end(),[](auto &a,auto &b){
            return a.second>b.second;
        });

        ofstream out("wordCount.txt");
        for(auto &p:v){
        out<<p.first<<" "<<to_string(p.second)<<"\n";;
        }
        out.close();
    }
    else{
        string local_text = receive(0);
        map<string,int> local_count = wordCount(local_text);
        send(map_to_string(local_count),0);
    }
    MPI_Finalize();
    return 0;
}