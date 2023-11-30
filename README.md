## 实验记录

### Project 0

任务0是要实现一个kv字典树，并且这个字典树可以存储任意类型的数据

![1701314510627](image/README/1701314510627.png)

##### TrieNode类

TrieNode是我们要实现的第一个类，为了实现char->child的映射，使用了

```cpp
std::unordered_map<char, std::unique_ptr<TrieNode>> children_;

// 注意，这里统一使用了unique_ptr智能指针对数据进行管理，这样的好处就是TrieNode只能当一颗Trie的节点，不会出现连接到其他Trie上的情况
```

* [ ] `TrieNode(char key_char)`

- 构造函数，对一个节点赋值key，并且标记为非终端节点

* [ ] `TrieNode(TrieNode &&other_trie_node)`

- 移动拷贝构造函数

* [ ] `bool HasChild(char key_char)`

- 判断节点是否有key为key_char孩子节点，直接根据map进行查询即可

* [ ] bool HasChildren()

- 判断节点是否有任意一个孩子，直接判断map是不是empty

* [ ] bool IsEndNode()

- 直接返回是否为终端节点变量即可

* [ ] char GetKeyChar()

- 得到当前节点存储的key值

* [ ] std::unique_ptr `<TrieNode>` *InsertChildNode(charkey_char, std::unique_ptr `<TrieNode>` &&child)

- 插入孩子节点，先看chidlren_map中是否已经存在对应的key值了，日报存在直接返回null，如果不存在就直接插入即可。

* [ ] std::unique_ptr `<TrieNode>` *InsertChildNode(charkey_char, std::unique_ptr `<TrieNode>` &&child)

- 同理

* [ ] void RemoveChildNode(char key_char)

- 直接在chidren_map中删除就行了，不需要考虑内存管理，因为我们使用了unique_ptr对象，自动帮我们销毁对象

* [ ] void SetEndNode(bool is_end)

- 设置为终端节点

##### TrieNodeWithValue类

这个类继承自TrieNode，这个类比TrieNode类多了一个value属性，这个value是泛型类，可以支持任意类型的数据，构造函数方式有如下两种：

```cpp
explicit TrieNodeWithValue(TrieNode &&trieNode, T value) //基于TrieNode
explicit TrieNodeWithValue(char key_char, T value) // 重新构建

```

* [ ] T GetValue();

- 返回节点中的Value，因为Value属于T的泛型，所以要加上T

##### Trie类

Trie是最终的字典类，有一个关键的结构为root_，一切查找都从这里查，root_被初始化key_char为 `a`

* [ ] bool Insert(conststd::string &key, T value)

- 向Trie中插入一个kv对，思路就是从root开始逐层遍历，注意如果某个key已经存在，我们需要直接返回，原来的值不能被修改

* [ ] bool Remove(conststd::string &key)

- remove的思路很简单，使用递归来实现，如果递归到最后一个字符了，判断一下是否为终端节点，如果是终端节点，那么我们就返回false，如果是终端节点但是还有孩子节点，那么就不能直接删除了，我们需要直接将其标记为非终端节点即可

### Project 1

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
