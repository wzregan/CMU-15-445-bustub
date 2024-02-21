/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int start_index): bpm_(bpm), cursor_(start_index) {
    if (page_id==INVALID_PAGE_ID) {
        cursor_ = -1;
    }else{
        Page* page_data = bpm->FetchPage(page_id);
        page_ = reinterpret_cast<LeafPage*>(page_data->GetData());
    }
}


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { 
    if ( cursor_== -1) {
        return true;
    }
    if (page_->GetNextPageId()==INVALID_PAGE_ID && cursor_ == this->page_->GetSize()) {
        return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
if (IsEnd() && itr.IsEnd()) {
    return true;
}
return itr.page_->GetPageId() == this->page_->GetPageId() && itr.cursor_==this->cursor_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
    return this->page_->array_[cursor_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    cursor_++;
    if (IsEnd()) {
        return *this;
    }else if (cursor_ < page_->GetSize()) {
        return *this;
    }
    page_id_t next_page_id = page_->GetNextPageId();
    bpm_->UnpinPage(page_->GetPageId(), false);
    Page* page_data = bpm_->FetchPage(next_page_id);
    page_ = reinterpret_cast<LeafPage*>(page_data->GetData());
    cursor_ = 0;
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
