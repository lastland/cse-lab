// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

#define DEBUG

void extent_flusher::dorelease(lock_protocol::lockid_t lid)
{
    ec->flush(lid);
}

extent_client::extent_client(std::string dst)
{
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }
}

// a demo to show how to use RPC
extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
		       extent_protocol::attr &attr)
{
    extent_protocol::status ret = extent_protocol::OK;
    if (file_cache_pool.find(eid) == file_cache_pool.end())
    {
        std::string buf;
        ret = cl->call(extent_protocol::get, eid, buf);
        if (ret == extent_protocol::OK)
        {
            ret = cl->call(extent_protocol::getattr, eid, attr);
            file_cache_pool[eid] = file_cache(buf);
            file_cache_pool[eid].attr = attr;
        }
    }
    else
    {
        attr = file_cache_pool[eid].attr;
    }
    return ret;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::create, type, id);
    return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
    extent_protocol::status ret = extent_protocol::OK;
    if (file_cache_pool.find(eid) == file_cache_pool.end())
    {
        ret = cl->call(extent_protocol::get, eid, buf);
        file_cache_pool[eid] = file_cache(buf);
#ifdef DEBUG
        printf("get %s from remote\n", file_cache_pool[eid].content.c_str());
#endif
    }
    else
    {
        buf = file_cache_pool[eid].content;
#ifdef DEBUG
        printf("get %s on local with size %d\n",
               file_cache_pool[eid].content.c_str(),
               file_cache_pool[eid].content.size());
#endif
    }
    return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
    extent_protocol::status ret = extent_protocol::OK;
    int r;
    file_cache* f = &file_cache_pool[eid];
    f->content = buf;
    f->dirty = true;
    f->attr.ctime = f->attr.mtime = time(NULL);
    f->attr.size = buf.size();
#ifdef DEBUG
    printf("update: %s\n", f->content.c_str());
#endif
    return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
    extent_protocol::status ret = extent_protocol::OK;
    int r;
    file_cache_pool[eid].removed = true;
    return ret;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
    extent_protocol::status ret = extent_protocol::OK;

    if (file_cache_pool.find(eid) == file_cache_pool.end())
        return ret;

    file_cache* f = &file_cache_pool[eid];
    if (f->removed)
    {
        int r;
        ret = cl->call(extent_protocol::remove, eid, r);
    }
    else if (f->dirty)
    {
        int r;
#ifdef DEBUG
        printf("write back: %s\n", f->content.c_str());
#endif
        ret = cl->call(extent_protocol::put, eid, f->content, r);
    }
    file_cache_pool.erase(eid);
    return ret;
}
