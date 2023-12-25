#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

#define LeafPage BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define InternalPage BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> 

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
        // 先判断是否有根页面


      }

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  page_id_t root_page_id = GetRootPageId();
  Page * root_page = buffer_pool_manager_->FetchPage(root_page_id);
  BPlusTreePage * node = ToGeneralPage(root_page->GetData());
  node->GetPageId();

  // 判断是否为内部结点，如果是内部节点，那么就说明只有这一个节点

  return false;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Search(const KeyType &key, std::vector<ValueType> *result){
  // page_node不会为空
  Page * root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage * node = ToGeneralPage(root_page);
  while (!node->IsLeafPage()) {
    // page_node不是叶子节点，所以page_node可以转化为中间结点
    InternalPage *page_internal_node = ToInternalPage(node);
    // 通过中间节点找到合适的叶子结点，然后循环再次判断
    int idx;
    page_internal_node->BinarySearch(key, &idx, comparator_);
    
    page_id_t page_id = page_internal_node->ValueAt(idx);

    Page * page = buffer_pool_manager_->FetchPage(page_id);
    node = reinterpret_cast<InternalPage * >(page->GetData());
  }
  // 如果是叶子结点，那我们就可以搜索值了
  if (node->IsLeafPage()) {
    LeafPage* page_leaf_node = ToLeafPage(node);
    page_leaf_node->Get(key, comparator_,result);
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key,                                          
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 如果当前B+树为空，则先初始化一下
  if (IsEmpty()){
    // 如果root页还没设置，则初始化一下
    Page * page = buffer_pool_manager_->NewPage(&root_page_id_);
    // 将该页面设置为根页面
    UpdateRootPageId(root_page_id_);
    // 初始化为数据结点
    LeafPage* root_node = ToLeafPage(page->GetData());
    root_node->Init(root_page_id_, HEADER_PAGE_ID, leaf_max_size_);
  }
  // 1. 找到根节点
  page_id_t root_page_id = GetRootPageId();
  Page * page = buffer_pool_manager_->FetchPage(root_page_id);
  // 使用父类节点类接收这个结点
  BPlusTreePage* node = ToGeneralPage(page->GetData());
  
  while (!node->IsLeafPage()) {
      InternalPage * node_interal = ToInternalPage(node);
      // 2. 找到下一层开始的位置
      int idx;
      node_interal->BinarySearch(key, &idx, comparator_);
      // 如果是最后一个idx，则需要修改
      if (idx == node_interal->GetSize() && comparator_(key, node_interal->KeyAt(idx-1)) > 0) {
        node_interal->SetKeyAt(idx-1, key);
        idx = node_interal->GetSize() - 1;
      }
      // 3. 先取出page id
      page_id_t next_page_id = node_interal->ValueAt(idx);
      // 4. 然后取出page，并将其转化为BPlusPage
      Page * page_temp = buffer_pool_manager_->FetchPage(next_page_id);
      // 然后继续寻找
      node = ToGeneralPage(page_temp->GetData());
  }
  // 跳出for循环之后，肯定就是叶子节点了
  LeafPage * leaf_node = ToLeafPage(node);
  // 然后插入即可
  bool is_full = leaf_node->InsertRecard(key, value, comparator_);
  if (is_full) {
    // 如果本身就是根结点，需要插入一个中间节点
    if (leaf_node->IsRootPage()){
      // 创建一个一个新的页面
      Page * root_page = buffer_pool_manager_->NewPage(&root_page_id_);
      // 更新root的id
      UpdateRootPageId();
      // 转换成root节点
      InternalPage * root_page_node = ToInternalPage(root_page->GetData());
      // 初始化root节点
      root_page_node->Init(root_page_id_, HEADER_PAGE_ID, internal_max_size_);
      // 然后重新设置当前leaf的父节点
      leaf_node->SetParentPageId(root_page_id_);
    }
    // 1. 新建一个分裂节点的页面
    page_id_t splited_page_id;
    page = buffer_pool_manager_->NewPage(&splited_page_id);
    LeafPage* split_page_node = ToLeafPage(page->GetData());
    split_page_node->Init(splited_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    // 开始分裂
    SplitNode(leaf_node, split_page_node);
    // 然后取出其父亲节点
    Page * parent_page = buffer_pool_manager_->FetchPage(split_page_node->GetParentPageId());
    // 转化为内部结点
    InternalPage * parent_page_node = ToInternalPage(parent_page->GetData());
    // 然后把分裂的结点插入到内部结点中
    parent_page_node->Insert(leaf_node->KeyAt(leaf_node->GetSize()- 1), leaf_node->GetPageId(), comparator_);
    is_full = parent_page_node->Insert(split_page_node->KeyAt(split_page_node->GetSize() - 1), splited_page_id, comparator_);
    InternalPage * spliting_node = parent_page_node;
    while (is_full){
      // 建立分割节点
      page = buffer_pool_manager_->NewPage(&splited_page_id);

      InternalPage* temp_page = ToInternalPage(page->GetData());
      temp_page->Init(splited_page_id, spliting_node->GetParentPageId(), internal_max_size_);
      // 在对中间节点进行分裂
      SplitNode(spliting_node, temp_page);
      // 取出其父亲节点
      page_id_t spliting_parent_page_id = spliting_node->GetParentPageId();
      Page * spliting_parent_page;
      InternalPage * spliting_parent_page_node;
      if (spliting_parent_page_id == HEADER_PAGE_ID) {
        // 创建一个根结点
        spliting_parent_page = buffer_pool_manager_->NewPage(&root_page_id_);
        // 更新root的id
        UpdateRootPageId();
        // 转换成root节点
        spliting_parent_page_node = ToInternalPage(spliting_parent_page->GetData());
        // 初始化root节点
        spliting_parent_page_node->Init(root_page_id_, HEADER_PAGE_ID, internal_max_size_);
        // 然后重新设置当前leaf的父节点
        spliting_node->SetParentPageId(root_page_id_);
        temp_page->SetParentPageId(root_page_id_);
      }else{
        spliting_parent_page = buffer_pool_manager_->FetchPage(spliting_parent_page_id);
        // 转化为内部结点
        spliting_parent_page_node = ToInternalPage(spliting_parent_page->GetData());
      }
      is_full = spliting_parent_page_node->Insert(temp_page->KeyAt(temp_page->GetMinSize() - 1), temp_page->GetPageId(), comparator_);
      spliting_parent_page_node->Insert(spliting_node->KeyAt(split_page_node->GetMinSize() - 1), spliting_node->GetPageId(), comparator_);
      spliting_node = spliting_parent_page_node;
    }
  }
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
template<class T>
auto BPLUSTREE_TYPE::SplitNode(T * origin, T * new_page_node) -> void {
  // 1. 找到中间结点
  int min_size = origin->GetMinSize();
  // 2. 直接拷贝
  memcpy(static_cast<void*>(new_page_node->array_), static_cast<void*>(origin->array_), sizeof(origin->array_[0]) * min_size);
  new_page_node->IncreaseSize(min_size);
  // 3. 删除本结点拷贝后的数据
  for (int i = 0; i < origin->GetSize() - min_size; i++) {
    origin->array_[i] = origin->array_[min_size + i];
  }
  // 4. 更新节点数据量
  origin->IncreaseSize(-min_size);

}



/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::ToInternalPage(T *page_data) -> InternalPage * { return reinterpret_cast<InternalPage *>(page_data); }

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::ToGeneralPage(T *page_data) -> BPlusTreePage * { return reinterpret_cast<BPlusTreePage *>(page_data); }


INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::ToLeafPage(T *page_data) -> LeafPage * { return reinterpret_cast<LeafPage*>(page_data); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
