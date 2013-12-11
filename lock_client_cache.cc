// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#define DEBUG

int lock_client_cache::last_port = 0;

static void* revokethread(void* x)
{
    lock_client_cache *c = (lock_client_cache*)x;
    c->revoker();
    return 0;
}

static void* retrythread(void* x)
{
    lock_client_cache *c = (lock_client_cache*)x;
    c->retryer();
    return 0;
}

lock_client_cache::lock_client_cache(std::string xdst,
				     class lock_release_user *_lu)
    : lock_client(xdst), lu(_lu)
{
    srand(time(NULL)^last_port);
    rlock_port = ((rand()%32000) | (0x1 << 10));
    const char *hname;
    // VERIFY(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs *rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

    pthread_t revoke_thread, retry_thread;
    VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
    VERIFY(pthread_mutex_init(&revoke_mutex, NULL) == 0);
    VERIFY(pthread_mutex_init(&retry_mutex, NULL) == 0);
    VERIFY(pthread_cond_init(&revoke_cond, 0) == 0);
    VERIFY(pthread_cond_init(&retry_cond, 0) == 0);
    VERIFY(pthread_create(&retry_thread, NULL, &retrythread, (void *)this) == 0);
    VERIFY(pthread_create(&revoke_thread, NULL, &revokethread, (void *)this) == 0);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
#ifdef DEBUG
    char cid[100];
    strcpy(cid, id.c_str());
    tprintf("-* acquire on %s, %d\n", cid, pthread_self());
#endif
    int ret = lock_protocol::OK, r;
    lock_cache* c;

#ifdef DEBUG
    tprintf("%s acquires lock %d\n", cid, lid);
#endif

    pthread_mutex_lock(&mutex);
    if (locks.find(lid) == locks.end())
    {
        locks[lid] = lock_cache();
    }
    c = &locks[lid];
    pthread_mutex_unlock(&mutex);

    pthread_mutex_lock(&c->mutex);
 retry:
#ifdef DEBUG
    tprintf("%s is going to try acuiring lock %d\n", cid, lid);
    tprintf("current state of lock on %s is %d\n", cid, c->state);
#endif
    switch (c->state)
    {
    case NONE:
        c->state = ACQUIRING;
        ret = cl->call(lock_protocol::acquire, lid, id, r);
        if (ret == lock_protocol::RETRY)
            goto retry;
    case FREE:
        c->state = LOCKED;
        c->owner = pthread_self();
#ifdef DEBUG
        tprintf("%s is granted with lock %d\n", cid, lid);
#endif
        break;
    default:
#ifdef DEBUG
        tprintf("****** %s waits to retry on lock %d\n", cid, lid);
#endif
        VERIFY(pthread_cond_wait(&c->ac_cond, &c->mutex) == 0);
        if (c->state == FREE)
        {
            c->state = LOCKED;
            c->owner = pthread_self();
#ifdef DEBUG
            tprintf("%s is granted with lock %d\n", cid, lid);
#endif
        }
        else
            goto retry;
        break;
    }
    pthread_mutex_unlock(&c->mutex);

    return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
#ifdef DEBUG
    char cid[100];
    strcpy(cid, id.c_str());
    tprintf("-*release on %s %d\n", id.c_str(), pthread_self());
#endif
    lock_cache* c;
#ifdef DEBUG
    tprintf("%s asks to release lock %d\n", cid, lid);
#endif
    pthread_mutex_lock(&mutex);
    VERIFY(locks.find(lid) != locks.end());
    c = &locks[lid];
    pthread_mutex_unlock(&mutex);

    pthread_mutex_lock(&c->mutex);
#ifdef DEBUG
    tprintf("state of lock is %d\n", c->state);
#endif
    if (c->state == LOCKED)
    {
        c->state = FREE;
#ifdef DEBUG
        tprintf("%s released lock %d\n", cid, lid);
#endif
        pthread_cond_signal(&c->ac_cond);
    }
    else if (c->state == RELEASING)
    {
        pthread_cond_signal(&c->rl_cond);
    }
    pthread_mutex_unlock(&c->mutex);

    return lock_protocol::OK;
}

void lock_client_cache::revoker()
{
    int r, ret;
    while (true)
    {
#ifdef DEBUG
        tprintf("revoker on %s\n", id.c_str());
#endif
        pthread_mutex_lock(&revoke_mutex);
        VERIFY(pthread_cond_wait(&revoke_cond, &revoke_mutex) == 0);
        while (!revoke_list.empty())
        {
            lock_protocol::lockid_t lid = revoke_list.front();
            revoke_list.pop_front();
#ifdef DEBUG
            tprintf("%s is going to revoke %d\n", id.c_str(), lid);
#endif
            VERIFY(locks.find(lid) != locks.end());
            lock_cache* c = &locks[lid];
            pthread_mutex_lock(&c->mutex);
#ifdef DEBUG
            tprintf("revoker: current state of lock on %s is %d\n",
                    id.c_str(), c->state);
#endif
            if (c->state == LOCKED || c->state == ACQUIRING)
            {
#ifdef DEBUG
                tprintf("revoker: current state of lock on %s is %d\n",
                        id.c_str(), c->state);
                tprintf("revoker: lock is in use, wait for releasing\n");
#endif
                c->state = RELEASING;
                VERIFY(pthread_cond_wait(&c->rl_cond, &c->mutex) == 0);
            }
            c->state = NONE;
            lu->dorelease(lid);
            ret = cl->call(lock_protocol::release, lid, id, r);
#ifdef DEBUG
            tprintf("%s revoked %d\n", id.c_str(), lid);
#endif
            pthread_cond_signal(&c->ac_cond);
            pthread_mutex_unlock(&c->mutex);
        }
        pthread_mutex_unlock(&revoke_mutex);
    }
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{
    int ret = rlock_protocol::OK;
#ifdef DEBUG
    tprintf("%s add %d to revoke_list\n", id.c_str(), lid);
#endif
    revoke_list.push_back(lid);
    VERIFY(pthread_cond_signal(&revoke_cond) == 0);

    return ret;
}


void lock_client_cache::retryer()
{
#ifdef DEBUG
    tprintf("retryer start.\n");
#endif
    int r, ret;
    while (true)
    {
#ifdef DEBUG
        tprintf("retryer on %s\n", id.c_str());
#endif
        pthread_mutex_lock(&retry_mutex);
        VERIFY(pthread_cond_wait(&retry_cond, &retry_mutex) == 0);
        while (!retry_list.empty())
        {
            lock_protocol::lockid_t lid = retry_list.front();
            lock_cache* c = &locks[lid];
            pthread_mutex_lock(&c->mutex);
#ifdef DEBUG
            tprintf("%s is going to retry on lock %d\n", id.c_str(), lid);
#endif
            retry_list.pop_front();
            VERIFY(locks.find(lid) != locks.end());
            ret = cl->call(lock_protocol::acquire, lid, id, r);
            if (ret == lock_protocol::OK)
            {
                if (c->state != RELEASING)
                    c->state = FREE;
                pthread_cond_signal(&c->ac_cond);
            }
            pthread_mutex_unlock(&c->mutex);
        }
        pthread_mutex_unlock(&retry_mutex);
    }
}


rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &)
{
    int ret = rlock_protocol::OK;
#ifdef DEBUG
    tprintf("%s add %d to retry list\n", id.c_str(), lid);
#endif
    retry_list.push_back(lid);
    VERIFY(pthread_cond_signal(&retry_cond) == 0);

    return ret;
}
