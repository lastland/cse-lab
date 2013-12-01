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
    REVOKING
} lock_state;

struct _lock
{
    lock_state state;
    std::string owner;
    pthread_mutex_t mutex;
    std::list<std::string> retry_list;

    _lock()
    {
        state = FREE;
        owner = "";
        VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
    }

    ~_lock()
    {
        VERIFY(pthread_mutex_destroy(&mutex) == 0);
    }
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
    std::list<lock_protocol::lockid_t> revoke_lock_list;
    std::list<lock_protocol::lockid_t> retry_lock_list;
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
