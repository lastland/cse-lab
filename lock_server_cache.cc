// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

//#define DEBUG

typedef _lock lock;

template<typename A, typename B>
std::pair<B, A> flip_pair(const std::pair<A, B> &p)
{
    return std::pair<B, A>(p.second, p.first);
}

void lock_server_cache::revoke()
{
    int r;
    rlock_protocol::status r_ret;
    while (true)
    {
        ScopedLock ml(&revoke_mutex);
        pthread_cond_wait(&revoke_cond, &revoke_mutex);
#ifdef DEBUG
        tprintf("start to revoke!\n");
#endif
        while (!revoke_lock_list.empty())
        {
            lock_protocol::lockid_t lid = revoke_lock_list.front();
            revoke_lock_list.remove(lid);
            lock* l = &locks[lid];
#ifdef DEBUG
            tprintf("revoke %s of %d\n", l->owner.c_str(), lid);
#endif
            handle h(l->owner);
            if (h.safebind())
                r_ret = h.safebind()->call(rlock_protocol::revoke,
                                           lid, r);
            if (!h.safebind() || r_ret != rlock_protocol::OK)
                tprintf("revoke rpc failed!\n");
        }
    }
}

void lock_server_cache::retry()
{
    int r;
    rlock_protocol::status r_ret;
    while (true)
    {
        ScopedLock ml(&retry_mutex);
        pthread_cond_wait(&retry_cond, &retry_mutex);
#ifdef DEBUG
        tprintf("start to retry!\n");
#endif
        while (!retry_lock_list.empty())
        {
#ifdef DEBUG
            tprintf("retry_lock_list size = %d\n", retry_lock_list.size());
#endif
            lock_protocol::lockid_t lid = retry_lock_list.front();
            retry_lock_list.pop_front();
            lock* l = &locks[lid];
            ScopedLock ml(&l->mutex);
#ifdef DEBUG
            tprintf("retry_list size = %d, lid = %d\n", l->retry_list.size(), lid);
#endif
            if (l->retry_list.empty())
            {
                l->state = FREE;
            }
            else
            {
                std::map<std::string, int> count;
                for (std::list<std::string>::iterator it = l->retry_list.begin();
                     it != l->retry_list.end(); it++)
                {
                    if (count.find(*it) == count.end())
                        count[*it] = 1;
                    else count[*it] += 1;
                }
                int maxcount = 0;
                std::string rid = "";
                for (std::map<std::string, int>::iterator it = count.begin();
                     it != count.end(); it++)
                {
                    if (it->second > maxcount)
                    {
                        rid = it->first;
                        maxcount = it->second;
                    }
                }
                l->retry_list.remove(rid);
#ifdef DEBUG
                tprintf("allow %s to retry\n", rid.c_str());
#endif

                handle h(rid);
                if (h.safebind())
                    r_ret = h.safebind()->call(rlock_protocol::retry,
                                               lid, r);
                if (!h.safebind() || r_ret != rlock_protocol::OK)
                {
                    tprintf("retry rpc failed!\n");
                }
                else
                {
                    l->state = LOCKED;
                    l->owner = rid;
#ifdef DEBUG
                    tprintf("give lock to %s\n", rid.c_str());
#endif
                    if (!l->retry_list.empty())
                    {
                        revoke_lock_list.push_back(lid);
                        pthread_cond_signal(&revoke_cond);
                    }
                }
            }
        }
    }
}

static void* revokethread(void* x)
{
    lock_server_cache* c = (lock_server_cache*)x;
    c->revoke();
    return 0;
}

static void* retrythread(void* x)
{
    lock_server_cache* c = (lock_server_cache*)x;
    c->retry();
    return 0;
}

lock_server_cache::lock_server_cache()
{
    pthread_t revoke_thread, retry_thread;
    VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
    VERIFY(pthread_mutex_init(&retry_mutex, NULL) == 0);
    VERIFY(pthread_mutex_init(&revoke_mutex, NULL) == 0);
    VERIFY(pthread_cond_init(&retry_cond, 0) == 0);
    VERIFY(pthread_cond_init(&revoke_cond, 0) == 0);
    VERIFY(pthread_create(&revoke_thread, NULL, &revokethread, (void *)this) == 0);
    VERIFY(pthread_create(&retry_thread, NULL, &retrythread, (void *)this) == 0);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int &)
{
#ifdef DEBUG
    char cid[100];
    strcpy(cid, id.c_str());
#endif
    lock_protocol::status ret = lock_protocol::OK;
    lock* l;

#ifdef DEBUG
    tprintf("%s acquires lock %d\n", cid, lid);
#endif
    ScopedLock ml1(&mutex);
    if (locks.find(lid) == locks.end())
    {
        locks[lid] = lock();
    }
    l = &locks[lid];

#ifdef DEBUG
    tprintf("current lock state is %d\n", l->state);
#endif

    ScopedLock ml(&l->mutex);
    switch (l->state)
    {
    case FREE:
        l->state = LOCKED;
        l->owner = id;
#ifdef DEBUG
        tprintf("gives %s lock %d\n", cid, lid);
#endif
        break;
    case LOCKED:
        l->state = REVOKING;
        revoke_lock_list.push_back(lid);
        pthread_cond_signal(&revoke_cond);
    default:
        l->retry_list.push_back(id);
#ifdef DEBUG
        tprintf("retry_list size = %d\n", l->retry_list.size());
#endif
        ret = lock_protocol::RETRY;
#ifdef DEBUG
        tprintf("gives %s RETRY\n", cid);
#endif
    }

    return ret;
}

int
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
                           int &r)
{
#ifdef DEBUG
    tprintf("%s is going to release %d\n", id.c_str(), lid);
#endif
    lock_protocol::status ret = lock_protocol::OK;
    lock* l;
    ScopedLock ml(&mutex);
    l = &locks[lid];
    if (l->owner != id)
    {
#ifdef DEBUG
        tprintf("something wrong! it's not the owner! owner = %s, id = %s\n",
                l->owner.c_str(), id.c_str());
#endif
        return lock_protocol::NOENT;
    }

#ifdef DEBUG
    tprintf("put lock %d in retry_lock_list\n", lid);
#endif
    retry_lock_list.push_back(lid);
    pthread_cond_signal(&retry_cond);

    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
    tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}
