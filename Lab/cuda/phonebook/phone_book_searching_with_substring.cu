%%writefile search_phonebook.cu
#include <bits/stdc++.h>
#include <cuda.h>
#include <cuda_runtime.h>

using namespace std;

#define MAX_STR_LEN 50

// Struct for sorting results on CPU
struct ResultContact {
    string name;
    string number;

    bool operator<(const ResultContact& other) const {
        return name < other.name;
    }
};

// Device substring check
__device__ bool check(const char* str1, const char* str2, int len) {
    for (int i = 0; str1[i] != '\0'; i++) {
        int j = 0;
        while (str1[i + j] != '\0' && j < len && str1[i + j] == str2[j]) {
            j++;
        }
        if (j == len) {
            return true;
        }
    }
    return false;
}

// CUDA kernel
__global__ void searchPhonebook(
    char* d_names,
    int num_contacts,
    char* search_name,
    int search_len,
    int* d_results
) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;

    if (idx < num_contacts) {
        char* current_name = d_names + idx * MAX_STR_LEN;
        d_results[idx] = check(current_name, search_name, search_len) ? 1 : 0;
    }
}

int main(int argc, char* argv[]) {

    if (argc != 3) {
        cerr << "Usage: " << argv[0]
             << " <search_string> <threads_per_block>\n";
        return 1;
    }

    string search_string = argv[1];
    int threads_per_block = atoi(argv[2]);

    string file_name = "/content/sample_data/phonebook1.txt";

    vector<string> host_names_vec;
    vector<string> host_numbers_vec;

    ifstream file(file_name);
    if (!file.is_open()) {
        cerr << "Error opening file: " << file_name << endl;
        return 1;
    }

    string line;
    while (getline(file, line)) {
        if (line.empty()) continue;

        int pos = line.find(",");
        if (pos == string::npos) continue;

        string name = line.substr(1, pos - 2);
        string number = line.substr(pos + 2, line.size() - pos - 3);

        host_names_vec.push_back(name);
        host_numbers_vec.push_back(number);
    }
    file.close();

    int num_contacts = host_names_vec.size();
    if (num_contacts == 0) {
        cerr << "No contacts found.\n";
        return 1;
    }

    // Host memory
    char* h_names = (char*)malloc(num_contacts * MAX_STR_LEN);
    int* h_results = (int*)malloc(num_contacts * sizeof(int));

    for (int i = 0; i < num_contacts; i++) {
        strncpy(h_names + i * MAX_STR_LEN,
                host_names_vec[i].c_str(),
                MAX_STR_LEN - 1);
        h_names[i * MAX_STR_LEN + MAX_STR_LEN - 1] = '\0';
    }

    // Device memory
    char *d_names, *d_search_name;
    int* d_results;

    int search_len = search_string.length();

    cudaMalloc(&d_names, num_contacts * MAX_STR_LEN);
    cudaMalloc(&d_results, num_contacts * sizeof(int));
    cudaMalloc(&d_search_name, search_len + 1);

    cudaMemcpy(d_names, h_names,
               num_contacts * MAX_STR_LEN,
               cudaMemcpyHostToDevice);

    cudaMemcpy(d_search_name, search_string.c_str(),
               search_len + 1,
               cudaMemcpyHostToDevice);

    int blocks = (num_contacts + threads_per_block - 1) / threads_per_block;

    // Kernel launch
    searchPhonebook<<<blocks, threads_per_block>>>(
        d_names, num_contacts, d_search_name, search_len, d_results
    );

    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) {
        cerr << "CUDA kernel error: "
             << cudaGetErrorString(err) << endl;
        return 1;
    }

    cudaDeviceSynchronize();

    cudaMemcpy(h_results, d_results,
               num_contacts * sizeof(int),
               cudaMemcpyDeviceToHost);

    vector<ResultContact> matched_contacts;
    for (int i = 0; i < num_contacts; i++) {
        if (h_results[i] == 1) {
            matched_contacts.push_back({
                host_names_vec[i],
                host_numbers_vec[i]
            });
        }
    }

    sort(matched_contacts.begin(), matched_contacts.end());

    cout << "\nSearch Results (Ascending Order):\n";
    for (const auto& c : matched_contacts) {
        cout << c.name << " " << c.number << endl;
    }

    // Cleanup
    free(h_names);
    free(h_results);
    cudaFree(d_names);
    cudaFree(d_results);
    cudaFree(d_search_name);

    return 0;
}
