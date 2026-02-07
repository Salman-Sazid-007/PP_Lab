#include <iostream>
#include <cuda_runtime.h>

using namespace std;

__global__ void matrixMul(float *A, float *B, float *R, int M, int N, int P, int batchOffset) {
  int k = threadIdx.x + batchOffset;
  float *a = A + k * M * N;
  float *b = B + k * N * P;
  float *r = R + k * M * P;
for(int outer = 0; outer < 100; outer++) {
  for(int i = 0; i < M; i++) {
    for(int l = 0; l < P; l++) {
      r[i * P + l] = 0.0f; // explicitly set to 0
      for(int j = 0; j < N; j++) {
        r[i * P + l] += a[i * N + j] * b[j * P + l];
      }
    }
  }
}
}

// print the first matrix only
void printMatrix(float *A, int M, int N) {
  for(int i = 0; i < M; i++) {
    for(int j = 0; j < N; j++) {
      printf("%.0f ", A[i * N + j]);
    }
    cout<<endl;
  }
}

int main(int argc, char* argv[]) {
  int threads = atoi(argv[1]);
  int K = atoi(argv[2]);
  int M = atoi(argv[3]);
  int N = atoi(argv[4]);
  int P = atoi(argv[5]);

  //int K = 2, M = 2, N = 2, P = 2;

  int size_of_a = K * M * N;
  int size_of_b = K * N * P;
  int size_of_r = K * M * P;

  float *h_A = (float*)malloc(size_of_a * sizeof(float));
  float *h_B = (float*)malloc(size_of_b * sizeof(float));
  float *h_R = (float*)malloc(size_of_r * sizeof(float));

  for(int i = 0; i < size_of_a; i++) {
    h_A[i] = rand() % 10;
  }
  for(int i = 0; i < size_of_b; i++) {
    h_B[i] = rand() % 10;
  }
  float *d_A;
  cudaMalloc(&d_A, size_of_a * sizeof(float));
  cudaMemcpy(d_A, h_A, size_of_a * sizeof(float), cudaMemcpyHostToDevice);

  float *d_B;
  cudaMalloc(&d_B, size_of_b * sizeof(float));
  cudaMemcpy(d_B, h_B, size_of_b * sizeof(float), cudaMemcpyHostToDevice);

  float *d_R;
  cudaMalloc(&d_R, size_of_r * sizeof(float));
  cudaMemset(d_R, 0, size_of_r * sizeof(float));

  int remainingMatrices = K;
  int batchOffset = 0;

  while(remainingMatrices > 0) {
    int currentBatchSize = min(remainingMatrices, threads);
    matrixMul<<<1, currentBatchSize>>>(d_A, d_B, d_R, M, N, P, batchOffset);
    cudaDeviceSynchronize();
    remainingMatrices -= currentBatchSize;
    batchOffset += currentBatchSize;
  }

  cudaMemcpy(h_R, d_R, size_of_r * sizeof(float), cudaMemcpyDeviceToHost);

  cout<<"Matrix A[0]:"<<endl;
  printMatrix(h_A, M, N);
  cout<<"Matrix B[0]:"<<endl;
  printMatrix(h_B, N, P);
  cout<<"Matrix R[0]:"<<endl;
  printMatrix(h_R, M, P);
  cudaFree(d_A);
  cudaFree(d_B);
  cudaFree(d_R);
  free(h_A);
  free(h_B);
  free(h_R);
  return 0;
}

//!nvcc -arch=sm_75 matrix.cu -o matrix
//!time ./matrix 400 2 2 2 2 > output.txt
