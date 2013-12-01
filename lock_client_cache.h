// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"


// Classes that inherit lock_release_user can override dorelease so that
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
    virtual void dorelease(lock_protocol::lockid_t) = 0;
    virtual ~lock_release_user() {};
};

typedef enum
{
    NONE,
    FREE,
    LOCKED,
    ACQUIRING,
    RELEASING
} lock_state;

typedef struct lock_cache
{
    lock_state state;
    pthread_t owner;
    pthread_mutex_t mutex;
    pthread_cond_t ac_cond;
    pthread_cond_t rl_cond;

    lock_cache()
    {
        state = NONE;
        VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
        VERIFY(pthread_cond_init(&ac_cond, 0) == 0);
        VERIFY(pthread_cond_init(&rl_cond, 0) == 0);
    }

    ~lock_cache()
    {
        VERIFY(pthread_mutex_destroy(&mutex) == 0);
        VERIFY(pthread_cond_destroy(&ac_cond) == 0);
        VERIFY(pthread_cond_destroy(&rl_cond) == 0);
    }
} lock_cache;

class lock_client_cache : public lock_client {
 private:
    class lock_release_user *lu;
    int rlock_port;
    std::string hostname;
    std::string id;
    std::map<lock_protocol::lockid_t, lock_cache> locks;
    pthread_mutex_t ac_mutex;
    pthread_mutex_t rl_mutex;
    pthread_mutex_t revoke_mutex;
    pthread_mutex_t retry_mutex;
    pthread_cond_t revoke_cond;
    pthread_cond_t retry_cond;
    std::list<lock_protocol::lockid_t> revoke_list;
    std::list<lock_protocol::lockid_t> retry_list;
 public:
    static int last_port;
    lock_client_cache(std::string xdst, class lock_release_user *l = 0);
    virtual ~lock_client_cache() {};
    lock_protocol::status acquire(lock_protocol::lockid_t);
    lock_protocol::status release(lock_protocol::lockid_t);
    rlock_protocol::status revoke_handler(lock_protocol::lockid_t,
                                          int &);
    rlock_protocol::status retry_handler(lock_protocol::lockid_t,
                                         int &);
    void revoker();
    void retryer();
};


#endif
