#include "ps/ps.h"
using namespace ps;

template <typename Val>
struct KVServerHandle {
  void operator()(KVServer<Val>* server) {
    std::vector<std::thread> resp_threads;
    int num_workers = Postoffice::Get()->num_workers();
    for(int i = 0 ; i < num_workers; i++) {
        resp_threads.push_back(std::thread([server,i] {
            KVPairs<Val> req_data;    
            for(int k = 0 ; k < 500; k++) {
              server->Receive(req_data,i); 
              size_t n = req_data.keys.size();
              CHECK_EQ(n, req_data.vals.size());
              KVPairs<Val> res;
              res.keys = req_data.keys;
              res.vals = req_data.vals;
              server->Send(res,i);
              }
      }));
    }
    std::for_each(resp_threads.begin(), 
      resp_threads.end(), 
      [](std::thread &resq_thread) {
      resq_thread.join();
    });
  }
};

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<float>(0, 
    KVServerHandle<float>());
  RegisterExitCallback([server](){ delete server; });
}

void RunWorker() {
  if (!IsWorker()) return;
  KVWorker<float> kv(0);

  // init
  int num = 10000;
  SArray<Key> keys(num);
  SArray<float> vals(num);

  int rank = MyRank();
  srand(rank + 7);

  // push
  int repeat = 500;
  SArray<float> rets;
  SArray<int> lens;
  for (int i = 0; i < repeat; ++i) {
  for (int i = 0; i < num; ++i) {
    keys[i] = kMaxKey / num * i +  rank;
    vals[i] = (rand() % 1000);
  }
    kv.Push(keys, vals);
    kv.Pull(keys,rets,lens);
    for(int i = 0 ; i < num; i++)
        CHECK_EQ(vals[i],rets[i]);
  }
  //for (int i = 0; i < num; ++i) {
  //  res += fabs(rets[i] - vals[i] * repeat);
  //}
  //CHECK_LT(res / repeat, 1e-5);
  //LL << "error: " << res / repeat;
}

int main(int argc, char *argv[]) {
  // setup server nodes
  StartServer();
  // start system
  Start();
  // run worker nodes
  RunWorker();
  // stop system
  Finalize();
  return 0;
}
