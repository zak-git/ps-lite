/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_SIMPLE_APP_H_
#define PS_SIMPLE_APP_H_
#include <string>
#include "ps/ps.h"
#include "ps/internal/message.h"
#include "ps/internal/postoffice.h"
namespace ps {

/**
 * \brief a simple app
 *
 * It provides basic communcation with a pair of int (head) and string (body)
 */
class SimpleApp {
 public:
  /**
   * \brief constructor
   * @param app_id the app id, should match with the remote node app with which this app
   * is communicated
   */
  explicit SimpleApp(int app_id);

  /** \brief deconstructor */
  virtual ~SimpleApp() { delete obj_; obj_ = nullptr; }

  /**
   * \brief wait until a request is finished
   *
   * \param timestamp
   */
  virtual inline void Wait(int timestamp) { obj_->WaitRequest(timestamp); }

  /**
   * \brief returns the customer
   */
  virtual inline Customer* get_customer() { return obj_; }

 protected:
  /** \brief empty construct */
  inline SimpleApp() : obj_(nullptr) {
    //if this is a worker,set recv_queues'size to num_servers
    //else if this is a sercer,set recv_queues'size to num_workers
    if(Postoffice::Get()->is_worker()) {
      int num_servers = Postoffice::Get()->num_servers();
      recv_queues_ = std::vector<ThreadsafeQueue<Message>>(num_servers);
    }
    else if(Postoffice::Get()->is_server()) {
      int num_workers = Postoffice::Get()->num_workers();
      recv_queues_ = std::vector<ThreadsafeQueue<Message>>(num_workers);
    }
  }

  /** \brief process a received message */
  virtual inline void Process(const Message& msg);
  /** \brief send a message to peer with id */
  virtual inline void Send(Message& msg,int peer_rank);
  /** \brief blocking receive a message to peer with id */
  virtual inline void Receive(Message& msg,int peer_rank);
  /** \brief ps internal object */
  Customer* obj_;
  /** \brief vector of recv_queues that each recv_queue holds message sent by a peer */
  std::vector<ThreadsafeQueue<Message>> recv_queues_;
};

////////////////////////////////////////////////////////////////////////////////

inline SimpleApp::SimpleApp(int app_id) : SimpleApp() {
  using namespace std::placeholders;
  obj_ = new Customer(app_id, std::bind(&SimpleApp::Process, this, _1));
}


inline void SimpleApp::Process(const Message& msg) {
    int sender = msg.meta.sender;
    int sender_rank = Postoffice::Get()->IDtoRank(sender);
    recv_queues_[sender_rank].Push(msg);  
}

inline void SimpleApp::Send(Message& msg,int peer_rank) {
    if(Postoffice::Get()->is_worker()) {
      msg.meta.recver = Postoffice::Get()->ServerRankToID(peer_rank);
      msg.meta.sender = Postoffice::Get()->WorkerRankToID(
              Postoffice::Get()->my_rank());
      msg.meta.customer_id = obj_->id();
      //msg.meta.timestamp = timestamp;
      Postoffice::Get()->van()->Send(msg);
    } 
    else if(Postoffice::Get()->is_server()) {
      msg.meta.recver = Postoffice::Get()->WorkerRankToID(peer_rank);
      msg.meta.sender = Postoffice::Get()->ServerRankToID(
              Postoffice::Get()->my_rank());
      msg.meta.customer_id = obj_->id();
      //msg.meta.timestamp = timestamp;
      Postoffice::Get()->van()->Send(msg);
    } 
}

inline void SimpleApp::Receive(Message& msg,int peer_rank) {
    if(Postoffice::Get()->is_worker()) {
      recv_queues_[peer_rank].WaitAndPop(&msg);
      CHECK_EQ(msg.meta.sender,Postoffice::Get()->ServerRankToID(peer_rank));
      //CHECK_EQ(msg.meta.timestamp,timestamp);
    }
    else if(Postoffice::Get()->is_server()) {
      recv_queues_[peer_rank].WaitAndPop(&msg);
      CHECK_EQ(msg.meta.sender,Postoffice::Get()->WorkerRankToID(peer_rank));
      //CHECK_EQ(msg.meta.timestamp,timestamp);
    }
}
}  // namespace ps
#endif  // PS_SIMPLE_APP_H_
