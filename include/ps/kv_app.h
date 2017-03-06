/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_KV_APP_H_
#define PS_KV_APP_H_
#include <algorithm>
#include <utility>
#include <vector>
#include "ps/base.h"
#include "ps/simple_app.h"
namespace ps {

/**
 * \brief the structure for a list of key-value pairs
 *
 * The keys must be unique and sorted in an increasing order.  The length of a
 * value can be more than one. If \a lens is empty, then the length
 * of a value is determined by `k=vals.size()/keys.size()`.  The \a i-th KV pair
 * is then
 *
 * \verbatim {keys[i], (vals[i*k], ..., vals[(i+1)*k-1])} \endverbatim
 *
 * If \a lens is given, then `lens[i]` is the length of the \a i-th
 * value. Let
 *
 * \verbatim n = lens[0] + .. + lens[i-1]  \endverbatim
 *
 * then the \a i-th KV pair is presented as
 *
 * \verbatim {keys[i], (vals[n], ..., vals[lens[i]+n-1])} \endverbatim
 */
template <typename Val>
struct KVPairs {
  // /** \brief empty constructor */
  // KVPairs() {}
  /** \brief the list of keys */
  SArray<Key> keys;
  /** \brief the according values */
  SArray<Val> vals;
  /** \brief the according value lengths (could be empty) */
  SArray<int> lens;
};

/**
 * \brief A worker node that can \ref Push (\ref Pull) key-value pairs to (from) server
 * nodes
 *
 * \tparam Val the type of value, which should be primitive types such as
 * int32_t and float
 */
template<typename Val>
class KVWorker : public SimpleApp {
 public:
  /** avoid too many this-> */
  using SimpleApp::obj_;
  /**
   * \brief constructor
   *
   * \param app_id the app id, should match with \ref KVServer's id
   */
  explicit KVWorker(int app_id) : SimpleApp(app_id) {
    using namespace std::placeholders;
    slicer_ = std::bind(&KVWorker<Val>::DefaultSlicer, this, _1, _2, _3);
  }

  /** \brief deconstructor */
  virtual ~KVWorker() { } 

  /**
   * \brief zero-copy Push
   *
   * This function is similar to \ref Push except that all data
   * will not be copied into system for better performance. It is the caller's
   * responsibility to keep the content to be not changed before actually
   * finished.
   */
  void Push(const SArray<Key>& keys,
            const SArray<Val>& vals,
            const SArray<int>& lens = {}) {
    KVPairs<Val> kvs;
    kvs.keys = keys;
    kvs.vals = vals;
    kvs.lens = lens;
    Send(kvs);
  }

  /**
   * \brief zero-copy Pull
   *
   * This function is similar to \ref Pull except that all data
   * will not be copied into system for better performance. It is the caller's
   * responsibility to keep the content to be not changed before actually
   * finished.
   */
  void Pull(const SArray<Key>& keys,
            SArray<Val>& vals,
            SArray<int>& lens) {
    KVPairs<Val> kvs;
    kvs.keys = keys;
    kvs.lens = lens;
    kvs.vals = vals;
    Receive(kvs);
    vals = kvs.vals;
  }
  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;
  /**
   * \brief a slicer partitions a key-value list according to the key ranges
   * \param send the kv list for partitioning
   * \param ranges the key ranges, ranges[i] is the key range of server i
   * \param sliced the sliced lists. slices[i] should only contains keys in
   * ranges[i] and the according values
   */
  using Slicer = std::function<void(
      const KVPairs<Val>& send, const std::vector<Range>& ranges,
      SlicedKVs* sliced)>;

  /**
   * \brief set a user-defined slicer
   */
  void set_slicer(const Slicer& slicer) {
    CHECK(slicer); slicer_ = slicer;
  }

private:
  /**
   * \brief send the kv list to all servers
   * @param timestamp the timestamp of the request
   */
  void Send(const KVPairs<Val>& kvs);
  /** \brief internal receive handle */
  void Receive(KVPairs<Val>& res);
  /** \brief default kv slicer */
  void DefaultSlicer(const KVPairs<Val>& send,
                     const std::vector<Range>& ranges,
                     SlicedKVs* sliced);

  /** \brief lock */
  std::mutex mu_;
  /** \brief kv list slicer */
  Slicer slicer_;
};


/**
 * \brief A server node for maintaining key-value pairs
 */
template <typename Val>
class KVServer : public SimpleApp {
public:
  /**
   * \brief the handle for responsing to worker 
   * \param recved the received message
   */
  using RespHandle = std::function<void(KVServer*)>;
  /**
   * \brief constructor
   * \param app_id the app id, should match with \ref KVWorker's id
   */
  explicit KVServer(int app_id,const RespHandle& resp_handle) : SimpleApp(app_id) {
      resp_thread_ = std::unique_ptr<std::thread>(new std::thread(resp_handle, this));   
  }

  /** \brief deconstructor */
  virtual ~KVServer() {
      resp_thread_->join();
  }
  /**
  * \brief send the kv list to all workers
  * @param timestamp the timestamp of this communication
  */
  void Send(const KVPairs<Val>& kvs,int worker_rank);
  /**
  * \brief recv the kv list from all workers
  * @param timestamp the timestamp of this communication
  */
  void Receive(KVPairs<Val>& res,int worker_rank);
private:
  std::unique_ptr<std::thread> resp_thread_;
};

/**
 * \brief an example handle adding pushed kv into store
 */
template <typename Val>
struct KVServerDefaultHandle {
  void operator()(KVServer<Val>* server) {
    std::vector<std::thread> resp_threads;
    int num_workers = Postoffice::Get()->num_workers();
    for(int i = 0 ; i < num_workers; i++) {
        resp_threads.push_back(std::thread([server,i] {
          while(true) {
              KVPairs<Val> req_data;
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
///////////////////////////////////////////////////////////////////////////////

template <typename Val>
void KVServer<Val>::Send(const KVPairs<Val>& kvs,int worker_rank) {
  Message msg;
  if (kvs.keys.size()) {
    msg.AddData(kvs.keys);
    msg.AddData(kvs.vals);
    if (kvs.lens.size()) {
      msg.AddData(kvs.lens);
    }
  }
  SimpleApp::Send(msg,worker_rank); 
}

template <typename Val>
void KVServer<Val>::Receive(KVPairs<Val>& res,int worker_rank) {
  Message msg;
  SimpleApp::Receive(msg,worker_rank);
  int n = msg.data.size();
  if (n) {
    CHECK_GE(n, 2);
    res.keys = msg.data[0];
    res.vals = msg.data[1];
    if (n > 2) {
      CHECK_EQ(n, 3);
      res.lens = msg.data[2];
      CHECK_EQ(res.lens.size(), res.keys.size());
    }
  }
}

template <typename Val>
void KVWorker<Val>::DefaultSlicer(
    const KVPairs<Val>& send, const std::vector<Range>& ranges,
    typename KVWorker<Val>::SlicedKVs* sliced) {
  sliced->resize(ranges.size());

  // find the positions in msg.key
  size_t n = ranges.size();
  std::vector<size_t> pos(n+1);
  const Key* begin = send.keys.begin();
  const Key* end = send.keys.end();
  for (size_t i = 0; i < n; ++i) {
    if (i == 0) {
      pos[0] = std::lower_bound(begin, end, ranges[0].begin()) - begin;
      begin += pos[0];
    } else {
      CHECK_EQ(ranges[i-1].end(), ranges[i].begin());
    }
    size_t len = std::lower_bound(begin, end, ranges[i].end()) - begin;
    begin += len;
    pos[i+1] = pos[i] + len;

    // don't send it to severs for empty kv
    sliced->at(i).first = (len != 0);
  }
  CHECK_EQ(pos[n], send.keys.size());
  if (send.keys.empty()) return;

  // the length of value
  size_t k = 0, val_begin = 0, val_end = 0;
  if (send.lens.empty()) {
    k = send.vals.size() / send.keys.size();
    CHECK_EQ(k * send.keys.size(), send.vals.size());
  } else {
    CHECK_EQ(send.keys.size(), send.lens.size());
  }

  // slice
  for (size_t i = 0; i < n; ++i) {
    if (pos[i+1] == pos[i]) {
      sliced->at(i).first = false;
      continue;
    }
    sliced->at(i).first = true;
    auto& kv = sliced->at(i).second;
    kv.keys = send.keys.segment(pos[i], pos[i+1]);
    if (send.lens.size()) {
      kv.lens = send.lens.segment(pos[i], pos[i+1]);
      for (int l : kv.lens) val_end += l;
      kv.vals = send.vals.segment(val_begin, val_end);
      val_begin = val_end;
    } else {
      kv.vals = send.vals.segment(pos[i]*k, pos[i+1]*k);
    }
  }
}

template <typename Val>
void KVWorker<Val>::Send(const KVPairs<Val>& kvs) {
  // slice the message
  SlicedKVs sliced;
  slicer_(kvs, Postoffice::Get()->GetServerKeyRanges(), &sliced);

  // need to add response first, since it will not always trigger the callback
  int skipped = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (!sliced[i].first) ++skipped;
  }
  for (size_t i = 0; i < sliced.size(); ++i) {
    const auto& s = sliced[i];
    if (!s.first) continue;
    Message msg;
    const auto& kvs = s.second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      msg.AddData(kvs.vals);
      if (kvs.lens.size()) {
        msg.AddData(kvs.lens);
      }
    }
    SimpleApp::Send(msg, i);
  }
}


template <typename Val>
void KVWorker<Val>::Receive(KVPairs<Val>& res) {
  int num_servers = Postoffice::Get()->num_servers();
  // all received kv pairs from servers
  std::vector<KVPairs<Val>> recv_kvs;
  //flag that indicate whether there are length data in kv pairs
  bool has_length = false;
  // receive kv pairs from all servers
  for (size_t i = 0; i < num_servers; ++i) {
    Message msg;
    SimpleApp::Receive(msg, i);
    KVPairs<Val> data;
    int n = msg.data.size();
    if (n) {
      CHECK_GE(n, 2);
      data.keys = msg.data[0];
      data.vals = msg.data[1];
      if (n > 2) {
        CHECK_EQ(n, 3);
        data.lens = msg.data[2];
        CHECK_EQ(data.lens.size(), data.keys.size());
        has_length = true;
      }
    }
    recv_kvs.push_back(data);
  }
  size_t total_key = 0, total_val = 0;
  for(auto s : recv_kvs) {
    Range range = FindRange(res.keys, s.keys.front(), s.keys.back()+1);
      CHECK_EQ(range.size(), s.keys.size())
          << "unmatched keys size from one server";
      if (!res.lens.empty()) CHECK_EQ(s.lens.size(), s.keys.size());
      total_key += s.keys.size();
      total_val += s.vals.size();
  }
  // fill vals and lens
  std::sort(recv_kvs.begin(), recv_kvs.end(), [](
      const KVPairs<Val>& a, const KVPairs<Val>& b) {
              return a.keys.front() < b.keys.front();
  });
  
  if(res.vals.empty()) {
    res.vals.resize(total_val);
  } else {
      CHECK_EQ(res.vals.size(), total_val);
  }
  if(has_length) {
    if(res.lens.empty()) {
        res.lens.resize(total_key);
    } else {
        CHECK_EQ(res.lens.size(), total_key);
    }
  }
  
  //raw pointers point to vals and lens
  Val* p_vals = res.vals.data();
  int* p_lens = res.lens.data();
  for (const auto& s : recv_kvs) {
    memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
    p_vals += s.vals.size();
    if (p_lens) {
      memcpy(p_lens, s.lens.data(), s.lens.size() * sizeof(int));
      p_lens += s.lens.size();
    }
  }
}
}  // namespace ps
#endif  // PS_KV_APP_H_
