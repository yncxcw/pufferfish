#include<iostream>
#include<thread>
#include<vector>
#include<random>
using namespace std;

//size of memory in terms of GB
#define S 7*1024
//size of 1MB
#define M 1024*1024
//number of threads
#define N 7

char *p[S];

void access(int index){

std::cout<<"start "<<index<<endl;

int size=S/N;
int min =(index-1)*size;
int max =index*size-1;

if(index > N){
  return;
}

//std::random_device rd;
//std::mt19937_64 gen(rd());

//std::uniform_int_distribution<unsigned long long> dist;

int loop=0;

while(1){
    for(int i=0;i<size;i++)
        for(int j=0;j<M;j++)
            p[i+min][j]='0'+i&9;
      //int i = dist(gen)%size+min;
      //int n = dist(gen)%M;
      //p[i][n]='0'+n%9;     
//loop++;
}

return;
}


int main(){

//allocation

std::cout<<"start allocation"<<endl;
for(int i=0;i<S;i++){
     p[i]=new char[M];
  for(int n=0;n<M;n++)
     p[i][n]='a';
}

vector<thread> threads;
for(int i=1;i<=N;i++){
 threads.push_back(thread(&access,i));
}

for(auto& th : threads){
 th.join();
}

return 0;

}
