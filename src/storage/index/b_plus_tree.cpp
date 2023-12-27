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
  int old_result_size = result->size();
  Search(key, result);
  // 判断是否为内部结点，如果是内部节点，那么就说明只有这一个节点

  return result->size() - old_result_size;
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
    UnpinPageNode(page_internal_node,false);
    node = FetchPageNode<InternalPage>(page_id);
  }
  // 如果是叶子结点，那我们就可以搜索值了
  if (node->IsLeafPage()) {
    LeafPage* page_leaf_node = ToLeafPage(node);
    page_leaf_node->Get(key, comparator_,result);
  }
  UnpinPageNode(node,false);

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
    // 初始化为数据结点
    LeafPage* root_node = NewPageNode<LeafPage>(HEADER_PAGE_ID, leaf_max_size_);
    root_page_id_ = root_node->GetPageId();
    UpdateRootPageId(root_page_id_);
    UnpinPageNode(root_node, true);
  }
  // 1. 找到根节点
  BPlusTreePage* node = FetchPageNode<BPlusTreePage>(root_page_id_);
  
  int idx;
  while (!node->IsLeafPage()) {
      InternalPage * node_interal = ToInternalPage(node);
      // 2. 找到下一层开始的位置
      node_interal->BinarySearch(key, &idx, comparator_);
      // 如果是最后一个idx，则需要修改
      if (idx == node_interal->GetSize() && comparator_(key, node_interal->KeyAt(idx-1)) > 0) {
        node_interal->SetKeyAt(idx-1, key);
        idx = node_interal->GetSize() - 1;
        UnpinPageNode(node_interal, true);
      }else {
        UnpinPageNode(node_interal, false);
      }

      // 3. 先取出page id
      page_id_t next_page_id = node_interal->ValueAt(idx);
      // 然后继续寻找
      node = FetchPageNode<BPlusTreePage>(next_page_id);
  }
  // 跳出for循环之后，肯定就是叶子节点了
  LeafPage * leaf_node = ToLeafPage(node);
  // 然后插入即可
  bool is_full = leaf_node->InsertRecard(key, value, comparator_);

  if (is_full) {
    InternalPage * spliting_node = SplitPageNode(leaf_node);
    while (spliting_node){
      spliting_node = SplitPageNode(spliting_node);
    }
  } else {
    UnpinPageNode(leaf_node, true);
  }
  // std::cout <<  << std::endl;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template<class T>
auto BPLUSTREE_TYPE::FetchPageNode(page_id_t page_id) -> T* {
  Page * page = buffer_pool_manager_->FetchPage(page_id);
  T * node = reinterpret_cast<T*>(page->GetData());
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPageNode(InternalPage * spliting_node) -> InternalPage * {
  if (spliting_node->IsRootPage()) {
    InternalPage * root_page_node = NewPageNode<InternalPage>(HEADER_PAGE_ID, internal_max_size_);
    // 重新设置page_id
    root_page_id_ = root_page_node->GetPageId();
    // 然后重新设置当前leaf的父节点
    UpdateRootPageId();
    // 更新父亲节点
    spliting_node->SetParentPageId(root_page_id_);

    root_page_node->Insert(spliting_node->KeyAt(spliting_node->GetSize() - 1), spliting_node->GetPageId(), comparator_);
    UnpinPageNode(root_page_node, true);
  }
  // 建立分割节点
  InternalPage* temp_page = NewPageNode<InternalPage>(spliting_node->GetParentPageId(), internal_max_size_);
  // 在对中间节点进行分裂
  SplitNode(spliting_node, temp_page);
  // 取出其父亲节点
  page_id_t spliting_parent_page_id = spliting_node->GetParentPageId();
  Page * spliting_parent_page = buffer_pool_manager_->FetchPage(spliting_parent_page_id);
  InternalPage* spliting_parent_page_node = ToInternalPage(spliting_parent_page->GetData());
  // 然后把新节点插入到父节点中
  bool is_full = spliting_parent_page_node->Insert(temp_page->KeyAt(temp_page->GetSize() - 1), temp_page->GetPageId(), comparator_);
  if (is_full) {
    return spliting_parent_page_node;
  }
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPageNode(LeafPage *leaf_node) -> InternalPage * {
    // 如果本身就是根结点，需要插入一个中间节点
    if (leaf_node->IsRootPage()){
      InternalPage * root_page_node = NewPageNode<InternalPage>(HEADER_PAGE_ID, internal_max_size_);
      // 重新设置page_id
      root_page_id_ = root_page_node->GetPageId();
      // 然后重新设置当前leaf的父节点
      UpdateRootPageId();
      // 设置父亲节点
      leaf_node->SetParentPageId(root_page_id_);
      // 把分裂节点插入到新的根节点
      root_page_node->Insert(leaf_node->KeyAt(leaf_node->GetSize() - 1), leaf_node->GetPageId(), comparator_);
      UnpinPageNode(root_page_node, true);
    }
    // 1. 新建一个分裂节点的页面
    LeafPage* split_page_node = NewPageNode<LeafPage>(leaf_node->GetParentPageId(), leaf_max_size_);
    // 开始分裂
    SplitNode(leaf_node, split_page_node);
    // 然后取出其父亲节点
    Page * parent_page = buffer_pool_manager_->FetchPage(split_page_node->GetParentPageId());
    // 转化为内部结点
    InternalPage * parent_page_node = ToInternalPage(parent_page->GetData());
    // 然后把分裂的结点插入到内部结点中
    bool is_full = parent_page_node->Insert(split_page_node->KeyAt(split_page_node->GetSize() - 1), split_page_node->GetPageId(), comparator_);
    
    UnpinPageNode(split_page_node, true);
    UnpinPageNode(leaf_node, true);
    
    if (is_full) {
      return parent_page_node;
    }
    UnpinPageNode(parent_page_node, true);
    return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
template<class PageNode>
auto BPLUSTREE_TYPE::SplitPageNode(PageNode *node) -> InternalPage * {
    // 如果本身就是根结点，需要插入一个中间节点
    if (node->IsRootPage()){
      InternalPage * root_page_node = NewPageNode<InternalPage>(HEADER_PAGE_ID, internal_max_size_);
      // 重新设置page_id
      root_page_id_ = root_page_node->GetPageId();
      // 然后重新设置当前leaf的父节点
      UpdateRootPageId();
      // 设置父亲节点
      node->SetParentPageId(root_page_id_);
      // 把分裂节点插入到新的根节点
      root_page_node->Insert(node->KeyAt(node->GetSize() - 1), node->GetPageId(), comparator_);
      UnpinPageNode(root_page_node, true);
    }
    // 1. 新建一个分裂节点
    PageNode* split_page_node = NewPageNode<PageNode>(node->GetParentPageId(), node->GetMaxSize());
    // 开始分裂
    SplitNode(node, split_page_node);
    // 然后取出其父亲节点
    InternalPage * parent_page_node = FetchPageNode<InternalPage>(split_page_node->GetParentPageId());
    // 判断节点是否为叶子节点，如果是叶子节点，则我们需要更新next_page_id
    if (node->IsLeafPage()) {
        // 1. 将分裂出来的那个结点的next_page_id设置为split_page_node
        // 2. 找到分裂节点的前一个结点修改next_pageid
        //    必须是是排在最头部的叶子，那么就不做任何操作
        LeafPage * leaf_node = ToLeafPage(node);
        LeafPage * leaf_split_page_node = reinterpret_cast<LeafPage*>(split_page_node);
        
        
        // 取出新分裂节点的pre
        page_id_t leaf_node_pre_page_id = leaf_node->GetPrePageId();
        // 设置新分裂节点的next
        leaf_split_page_node->SetNextPageId(leaf_node->GetPageId());
        // 设置新分裂节点的pre
        leaf_split_page_node->SetPrePageId(leaf_node_pre_page_id);
        // 更新当前旧分裂节点的pre
        leaf_node->SetPrePageId(leaf_split_page_node->GetPageId());
        // 更新旧的分裂点pre的next
        if (leaf_node_pre_page_id!=INVALID_PAGE_ID)  {
          LeafPage * leaf_node_pre_page = FetchPageNode<LeafPage>(leaf_node_pre_page_id);
          leaf_node_pre_page->SetNextPageId(leaf_split_page_node->GetPageId());
          UnpinPageNode(leaf_node_pre_page, true);
        }
    }
    // 然后把分裂的结点插入到内部结点中
    bool is_full = parent_page_node->Insert(split_page_node->KeyAt(split_page_node->GetSize() - 1), split_page_node->GetPageId(), comparator_);
    
    UnpinPageNode(split_page_node, true);
    UnpinPageNode(node, true);
    
    if (is_full) {
      return parent_page_node;
    }
    UnpinPageNode(parent_page_node, true);
    return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
template<class T>
auto BPLUSTREE_TYPE::NewPageNode(page_id_t parent_page_id, int max_size) -> T* {
  int page_id;
  Page * page = buffer_pool_manager_->NewPage(&page_id);
  T* temp_page = reinterpret_cast<T*>(page->GetData());
  temp_page->Init(page_id, parent_page_id, max_size);
  return temp_page;
}

INDEX_TEMPLATE_ARGUMENTS
template<class T>
auto BPLUSTREE_TYPE::UnpinPageNode(T * node, bool is_dirty) -> void {
  page_id_t page_id = node->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, is_dirty);
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
  // 如果是中间节点，还需要更新孩子结点的父亲！
  if (!origin->IsLeafPage()) {
    UpdateChildrenParent(reinterpret_cast<InternalPage*>(new_page_node));
  }
  // 4. 更新节点数据量
  origin->IncreaseSize(-min_size);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpdateChildrenParent(InternalPage * parent) -> void {
  for (int i = 0 ; i < parent->GetSize(); i++) {
    page_id_t child_page_id = parent->ValueAt(i);
    BPlusTreePage * child_page = FetchPageNode<BPlusTreePage>(child_page_id);
    child_page->SetParentPageId(parent->GetPageId());
    UnpinPageNode(child_page,true);
  }

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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // page_node不会为空
  BPlusTreePage * node = FetchPageNode<BPlusTreePage>(root_page_id_);
  while (!node->IsLeafPage()) {
    // page_node不是叶子节点，所以page_node可以转化为中间结点
    InternalPage *page_internal_node = ToInternalPage(node);
    // 通过中间节点找到合适的叶子结点，然后循环再次判断
    page_id_t page_id = page_internal_node->ValueAt(0);
    UnpinPageNode(page_internal_node,false);
    node = FetchPageNode<InternalPage>(page_id);
  }
  // 如果是叶子结点，那我们就可以搜索值了
  page_id_t first_leafpage_id = node->GetPageId();
  UnpinPageNode(node,false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, first_leafpage_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
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
    UnpinPageNode(page_internal_node,false);
    node = FetchPageNode<InternalPage>(page_id);
  }
  page_id_t leaf_page_id = node->GetPageId();
  LeafPage *leaf_node = ToLeafPage(node);
  int cursor = 0;
  leaf_node->BinarySearch(key, &cursor, comparator_);
  UnpinPageNode(node,false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id, cursor);
}

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
