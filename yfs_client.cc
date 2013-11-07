// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define DEBUG

yfs_client::yfs_client()
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    if ((r = lookup(parent, name, found, ino_out)) != OK)
        return r;
    if (found) r = EXIST;
    else
    {
        if ((r = ec->create(extent_protocol::T_FILE, ino_out)) != OK)
            return r;
        std::string buf;
        if ((r = ec->get(parent, buf)) != OK)
            return r;
#ifdef DEBUG
        std::cout<<"get ec: "<<buf<<std::endl;
#endif
        std::ostringstream s;
        s << name << "/" << ino_out << "/";
        buf += s.str();
#ifdef DEBUG
        std::cout<<"write to ec: "<<buf<<std::endl;
#endif
        if ((r = ec->put(parent, buf)) != OK)
            return r;
    }

#ifdef DEBUG
    std::cout<<"create: "<<r<<std::endl;
#endif

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
#ifdef DEBUG
    std::cout<<"lookup for "<<name<<std::endl;
#endif
    std::string buf;
    if ((r = ec->get(parent, buf)) != OK)
        return r;
    std::size_t pos = buf.find(name);
#ifdef DEBUG
    std::cout<<"look up in "<<parent<<" "<<buf<<std::endl;
#endif
    if (pos == std::string::npos)
        found = false;
    else {
        found = true;
        size_t s = buf.find('/', pos);
        size_t t = buf.find('/', s + 1);
#ifdef DEBUG
        std::cout<<"s = "<<s<<", t = "<<t
                 <<", substr = "<<buf.substr(s, t - s)<<std::endl;
#endif
        ino_out = strtol(buf.substr(s + 1, t - s).c_str(), NULL, 10);
    }

#ifdef DEBUG
    std::cout<<"lookup: "<<r<<std::endl;
#endif

    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
#ifdef DEBUG
    std::cout<<"readdir\n";
#endif

    std::string buf;
    ec->get(dir, buf);
    size_t s = 0;
    while (s != std::string::npos)
    {
        dirent e;
        size_t t;
        if ((t = buf.find('/', s)) == std::string::npos)
            return IOERR;
        e.name = buf.substr(s, t - s);
        s = t + 1;
        if ((t = buf.find('/', s)) == std::string::npos)
            return IOERR;
#ifdef DEBUG
        std::cout<<"read dir: "<<e.name
                 <<" "<<buf.substr(s, t - s)<<std::endl;
#endif
        e.inum = strtol(buf.substr(s, t - s).c_str(), NULL, 10);
        s = t + 1;
        list.push_front(e);
    }

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    return r;
}
