#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

typedef enum
    {
        FREE,
        LOCKED,
        RETRYING,
        REVOKING
    } lock_state;

struct client
{
    std::string cid;
    lock_protocol::lockid_t lid;

    client(std::string _cid, lock_protocol::lockid_t _lid)
    {
        cid = _cid;
        lid = _lid;
    }
};

struct _lock
{
    lock_state state;
    std::string owner;
    std::list<std::string> waiters;
    std::string retryer;

    _lock()
    {
        state = FREE;
        owner = "";
    }

    ~_lock() {}
};

class lock_server_cache
{
 private:
    int nacquire;
    pthread_mutex_t mutex;
    pthread_mutex_t revoke_mutex;
    pthread_mutex_t retry_mutex;
    pthread_cond_t revoke_cond;
    pthread_cond_t retry_cond;
    std::list<client> revoke_list;
    std::list<client> retry_list;
    std::map<lock_protocol::lockid_t, _lock> locks;
 public:
    lock_server_cache();
    lock_protocol::status stat(lock_protocol::lockid_t, int &);
    int acquire(lock_protocol::lockid_t, std::string id, int &);
    int release(lock_protocol::lockid_t, std::string id, int &);
    void retry();
    void revoke();
};

#endif
