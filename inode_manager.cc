#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
    using_blocks[bnum] = 1;

    return bnum++;
}

void
block_manager::free_block(uint32_t id)
{
    using_blocks[--bnum] = 0;

    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  bnum = BLOCK_START_POS;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

#define WRITE_TO_BLOCK(ino, buf)                \
    {                                           \
        uint32_t id = bm->alloc_block();        \
        bm->write_block(id, buf);               \
        ino->blocks[ino->size] = id;            \
        ino->size++;                            \
    }

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
    static uint32_t inum = 1;
    inode_t* ino = (inode_t*)malloc(sizeof(inode_t));
    ino->type = type;
    ino->size = 0;
    // TODO: time
    put_inode(inum, ino);

    return inum++;
}

void
inode_manager::free_inode(uint32_t inum)
{
    /*
     * your lab1 code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     */

    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode*
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
    inode_t* ino = get_inode(inum);
    inode_t* ido = NULL;
    uint32_t bsize = BLOCK_SIZE * ino->size;
    if (ino->size >= NDIRECT) {
        ido = get_inode(ino->blocks[NDIRECT]);
        bsize += BLOCK_SIZE * ido->size;
    }
    *buf_out = (char*)malloc(bsize);
    uint32_t _size = std::min(ino->size, (unsigned int)NDIRECT);
    for (uint32_t i = 0; i < _size; i++)
        bm->read_block(ino->blocks[i], *buf_out + i * BLOCK_SIZE);
    if (ido != NULL)
        for (uint32_t i = 0; i < ido->size; i++)
            bm->read_block(ido->blocks[i], *buf_out + (i + NDIRECT) * BLOCK_SIZE);
    //*size = strlen(*buf_out);
    *size = strlen(*buf_out);

    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
    inode_t* ino = get_inode(inum);
    uint32_t _size = std::min(size, NDIRECT * BLOCK_SIZE);
    for (uint32_t i = 0; i < _size; i += BLOCK_SIZE) {
        WRITE_TO_BLOCK(ino, buf + i);
    }
    if (ino->size >= NDIRECT) {
        uint32_t new_inum = alloc_inode(ino->type);
        ino->blocks[NDIRECT] = new_inum;
        inode_t* ido = get_inode(new_inum);
        for (uint32_t i = 0; i < size - _size; i += BLOCK_SIZE) {
            WRITE_TO_BLOCK(ido, buf + i + _size);
        }
        put_inode(new_inum, ido);
    }
    put_inode(inum, ino);

    return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
    inode_t* ino = get_inode(inum);
    a.type = ino->type;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    a.size = ino->size;

    return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  return;
}
