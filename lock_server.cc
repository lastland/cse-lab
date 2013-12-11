// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"

#define DEBUG

using namespace std;

lock_server::lock_server():
    nacquire (0), locks ()
{
    VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
    VERIFY(pthread_cond_init(&cond, 0) == 0);
}

lock_server::~lock_server()
{
    VERIFY(pthread_mutex_destroy(&mutex) == 0);
    VERIFY(pthread_cond_destroy(&cond) == 0);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
    ScopedLock ml(&mutex);
    while (true)
    {
#ifdef DEBUG
        cout<<clt<<" asks for lock "<<lid<<endl;
#endif
        if (locks.find(lid) == locks.end() || !locks[lid])
        {
            locks[lid] = true;
            locks_owner[lid] = clt;
            r = 0;
#ifdef DEBUG
            cout<<clt<<" be granted with lock "<<lid<<endl;
#endif
            break;
        }
        else
        {
            VERIFY(pthread_cond_wait(&cond, &mutex) == 0);
        }
    }
    return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
    ScopedLock ml(&mutex);
    if (locks.find(lid) == locks.end() || !locks[lid] || locks_owner[lid] != clt)
    {
#ifdef DEBUG
        cout<<"error!"<<endl;
#endif
        return lock_protocol::RPCERR;
    }
    else
    {
#ifdef DEBUG
        cout<<"releasing "<<lid<<" of "<<clt<<endl;
#endif
        locks[lid] = false;
        locks_owner[lid] = 0;
        r = 0;
        VERIFY(pthread_cond_broadcast(&cond) == 0);
    }
    return lock_protocol::OK;
}
