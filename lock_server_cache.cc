// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

typedef _lock lock;

void lock_server_cache::revoke()
{
    int r;
    rlock_protocol::status r_ret;
    while (true)
    {
        pthread_mutex_lock(&revoke_mutex);
        pthread_cond_wait(&revoke_cond, &revoke_mutex);
#ifdef DEBUG
        tprintf("start to revoke!\n");
#endif
        while (!revoke_list.empty())
        {
            client req = revoke_list.front();
            revoke_list.pop_front();
#ifdef DEBUG
            tprintf("revoke %s of %d\n", req.cid.c_str(), req.lid);
#endif
            handle h(req.cid);
            if (h.safebind())
            {
                pthread_mutex_unlock(&revoke_mutex);
                r_ret = h.safebind()->call(rlock_protocol::revoke,
                                           req.lid, r);
                pthread_mutex_lock(&revoke_mutex);
            }
            if (!h.safebind() || r_ret != rlock_protocol::OK)
                tprintf("revoke rpc failed!\n");
        }
        pthread_mutex_unlock(&revoke_mutex);
    }
}

void lock_server_cache::retry()
{
    int r;
    rlock_protocol::status ret;
    while (true)
    {
        pthread_mutex_lock(&retry_mutex);
        pthread_cond_wait(&retry_cond, &retry_mutex);
        while (!retry_list.empty())
        {
            client req = retry_list.front();
            retry_list.pop_front();
            handle h(req.cid);
            if (h.safebind())
            {
#ifdef DEBUG
                tprintf("allow %s to retry on %d\n", req.cid.c_str(), req.lid);
#endif
                pthread_mutex_unlock(&retry_mutex);
                ret = h.safebind()->call(rlock_protocol::retry, req.lid, r);
                pthread_mutex_lock(&retry_mutex);
            }
            if (!h.safebind() || ret != rlock_protocol::OK)
                tprintf("retry RPC failed!\n");
        }
        pthread_mutex_unlock(&retry_mutex);
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
    pthread_mutex_lock(&mutex);
    if (locks.find(lid) == locks.end())
        locks[lid] = lock();
    l = &locks[lid];

#ifdef DEBUG
    tprintf("current lock state is %d\n", l->state);
    tprintf("current retryer is %s\n", l->retryer.c_str());
#endif

    if (l->state == FREE || (l->state == RETRYING &&
                             l->retryer == id))
    {
        if (l->state == RETRYING)
            l->waiters.pop_front();
        l->state = LOCKED;
        l->owner = id;
        if (!l->waiters.empty())
        {
            l->state = REVOKING;
            pthread_mutex_lock(&revoke_mutex);
            revoke_list.push_back(client(id, lid));
            pthread_cond_signal(&revoke_cond);
            pthread_mutex_unlock(&revoke_mutex);
        }
    }
    else
    {
        l->waiters.push_back(id);
        if (l->state == LOCKED)
        {
            l->state = REVOKING;
            pthread_mutex_lock(&revoke_mutex);
            revoke_list.push_back(client(l->owner, lid));
            pthread_cond_signal(&revoke_cond);
            pthread_mutex_unlock(&revoke_mutex);
        }

        ret = lock_protocol::RETRY;
    }
    pthread_mutex_unlock(&mutex);

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
    pthread_mutex_lock(&mutex);
    l = &locks[lid];
    if (l->owner != id)
    {
#ifdef DEBUG
        tprintf("something wrong! it's not the owner! owner = %s, id = %s\n",
                l->owner.c_str(), id.c_str());
#endif
        return lock_protocol::NOENT;
    }

    l->state = FREE;
#ifdef DEBUG
    tprintf("%d freed.\n", lid);
#endif
    if (!l->waiters.empty())
    {
        l->state = RETRYING;
        client req(l->waiters.front(), lid);
        l->retryer = req.cid;
        pthread_mutex_lock(&retry_mutex);
#ifdef DEBUG
        tprintf("put lock %d in retry_list\n", lid);
#endif
        retry_list.push_back(req);
        pthread_cond_signal(&retry_cond);
        pthread_mutex_unlock(&retry_mutex);
    }
    pthread_mutex_unlock(&mutex);

    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
    tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}
