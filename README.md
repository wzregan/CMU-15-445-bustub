## 实验记录

### Project 0

### project 1

#### 做前必读

**DiskManager类**

创建一个磁盘管理类，用于向指定的数据库db文件写入

主要需要注意的如下四个函数，实现其实很简单。

```cpp
virtual void WritePage(page_id_t page_id, const char *page_data);
virtual void ReadPage(page_id_t page_id, char *page_data);
void WriteLog(char *log_data, int size);
auto ReadLog(char *log_data, int size, int offset) -> bool;
```

**Page类**

Page类主要是对数据库系统中的存储单元做的一个封装，在这个类中主要有三个成员变量需要注意

```cpp
page_id_t page_id_ // 当前page页的id
int pin_count_ // 被线程使用的数量
bool is_dirty_ // 是否为脏页
```

因为数据库是多线程访问的，所以需要对page页做线程安全的保护，内部提供了一个读写锁，其实现原理也很简单，直接调库，使用 `std::shared_mutex` 即可。

#### 任务

##### extendible_hash_table


##### LRUK_Replacer


##### BufferPoolManagerInstance

* [X] `NewPgImp( page_id * page_id) -> Page *`

* 在缓存池中创建一个新的页，然后将这个页的page_id 设置到传进来的这个参数中，如果没有空闲的页框 且 没有可以驱逐的页，那么就返回一个空，否则就返回创建的这个页的地址

* [X] `auto FetchPgImp(page_id_t page_id) -> Page* `

  - 从缓存池中根据指定的page_id找到对应的page，然后返回这个page的内存地址。 首先从已有的缓冲池中找，如果找到了则返回，如果没找到则说明需要再磁盘中读取。 如果需要在磁盘中读取的话，必须保证缓存池中有可以驱逐的页，如果没有直接返回空值。 如果page_id无效值，也直接返回空值
* [X] `auto UnpinPgImp(page_id_t page_id,  bool is_dirty) -> booloverride`

  - 减少某个page pin的个数，如果 pin 个数已经是0或者page_id不存在 则返回false。 is_dirty可以用来设置页是否为脏页。
* [X] auto FlushPgImp(page_id_t page_id)  -> bool

  - 将指定的页刷新到磁盘中，注意刷新之后将dirty flag设置为false
* [X] ` void FlushAllPgsImp()`

  - 将所有页都刷新到磁盘中
* [X] `auto DeltePgImp(page_id_t page_id) -> bool`

  - 根据指定的page_id从缓冲池中删除，如果page的pin个数还大于0则说明有线程还在使用，则直接返回false。 当我们删除了这个page之后，这个page所在的lru也应该被删除。
