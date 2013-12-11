// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include "extent_server.h"
#include "lock_protocol.h"
#include "lock_client_cache.h"

class extent_flusher : public lock_release_user
{
 public:
    extent_flusher(class extent_client *_ec) { ec = _ec; };
    virtual void dorelease(lock_protocol::lockid_t);
    virtual ~extent_flusher() {};
 private:
    extent_client* ec;
};

class file_cache
{
 public:
    std::string content;
    bool dirty;
    bool removed;
    extent_protocol::attr attr;

    file_cache()
    {
        content = "";
        init();
    }

    file_cache(std::string buf)
    {
        content = buf;
        init();
    }

 private:
    void init()
    {
        dirty = removed = false;
    }
};

class extent_client {
 private:
    rpcc *cl;

    std::map<extent_protocol::extentid_t, file_cache> file_cache_pool;

 public:
    extent_client(std::string dst);

    extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
    extent_protocol::status get(extent_protocol::extentid_t eid,
                                std::string &buf);
    extent_protocol::status getattr(extent_protocol::extentid_t eid,
                                    extent_protocol::attr &a);
    extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
    extent_protocol::status remove(extent_protocol::extentid_t eid);
    extent_protocol::status flush(extent_protocol::extentid_t eid);
};

#endif
