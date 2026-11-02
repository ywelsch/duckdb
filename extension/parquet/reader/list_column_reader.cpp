#include <stdint.h>
#include <utility>

#include "duckdb/common/vector/list_vector.hpp"
#include "reader/list_column_reader.hpp"
#include "parquet_reader.hpp"
#include "column_reader.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "resizable_buffer.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {
struct ParquetColumnSchema;

//! A range of rows of one child read
struct ListChildPiece {
	ListChildPiece(const Vector &vector_p, idx_t offset_p, idx_t count_p)
	    : vector(vector_p.GetType(), vector_p.GetBufferRef()), offset(offset_p), count(count_p) {
	}

	Vector vector;
	idx_t offset;
	idx_t count;
};

//! A dictionary decoder reuses its selection vector for every read, so a read that is kept beyond the next read needs
//! its own copy of every dictionary selection in it
static void CopyDictionarySelections(Vector &vector, idx_t count) {
	switch (vector.GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		if (!DictionaryVector::DictionarySize(vector).IsValid()) {
			vector.Flatten();
			return;
		}
		auto &sel = DictionaryVector::SelVector(vector);
		SelectionVector owned_sel(count);
		for (idx_t i = 0; i < count; i++) {
			owned_sel.set_index(i, sel.get_index(i));
		}
		auto entry = vector.BufferMutable().Cast<DictionaryBuffer>().GetEntryPtr();
		vector.Dictionary(std::move(entry), owned_sel, count);
		return;
	}
	case VectorType::FLAT_VECTOR:
		switch (vector.GetType().InternalType()) {
		case PhysicalType::STRUCT:
			for (auto &entry : StructVector::GetEntries(vector)) {
				CopyDictionarySelections(entry, count);
			}
			return;
		case PhysicalType::LIST:
			CopyDictionarySelections(ListVector::GetChildMutable(vector), ListVector::GetListSize(vector));
			return;
		default:
			return;
		}
	default:
		return;
	}
}

static bool SharesStorageDictionary(vector<ListChildPiece> &pieces) {
	for (auto &piece : pieces) {
		if (piece.vector.GetVectorType() != VectorType::DICTIONARY_VECTOR ||
		    !DictionaryVector::DictionarySize(piece.vector).IsValid() ||
		    &DictionaryVector::Child(piece.vector) != &DictionaryVector::Child(pieces[0].vector)) {
			return false;
		}
	}
	return true;
}

static bool AllFlat(vector<ListChildPiece> &pieces) {
	for (auto &piece : pieces) {
		if (piece.vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
	}
	return true;
}

static void CopyValidity(vector<ListChildPiece> &pieces, Vector &result) {
	auto &result_validity = FlatVector::ValidityMutable(result);
	idx_t result_idx = 0;
	for (auto &piece : pieces) {
		auto &piece_validity = FlatVector::Validity(piece.vector);
		for (idx_t i = 0; i < piece.count; i++, result_idx++) {
			if (!piece_validity.RowIsValid(piece.offset + i)) {
				result_validity.SetInvalid(result_idx);
			}
		}
	}
}

//! Concatenates the child reads of one list read into "result". The child values of a dictionary-encoded column stay
//! a dictionary over the column's dictionary instead of being copied, also through nested structs and lists, so that
//! low-cardinality values inside lists and maps cost an index per value rather than a copy of the value.
static void ConcatenateListChildren(vector<ListChildPiece> &pieces, idx_t total, Vector &result) {
	D_ASSERT(!pieces.empty());
	if (pieces.size() == 1 && pieces[0].offset == 0 && pieces[0].count == pieces[0].vector.size()) {
		result.Reference(pieces[0].vector);
		return;
	}
	if (SharesStorageDictionary(pieces)) {
		SelectionVector sel(total);
		idx_t result_idx = 0;
		for (auto &piece : pieces) {
			auto &piece_sel = DictionaryVector::SelVector(piece.vector);
			for (idx_t i = 0; i < piece.count; i++) {
				sel.set_index(result_idx++, piece_sel.get_index(piece.offset + i));
			}
		}
		auto entry = pieces[0].vector.BufferMutable().Cast<DictionaryBuffer>().GetEntryPtr();
		result.Dictionary(std::move(entry), sel, total);
		return;
	}
	auto &type = result.GetType();
	if (type.InternalType() == PhysicalType::STRUCT && AllFlat(pieces)) {
		Vector struct_result(type, total);
		CopyValidity(pieces, struct_result);
		auto &result_entries = StructVector::GetEntries(struct_result);
		for (idx_t entry_idx = 0; entry_idx < result_entries.size(); entry_idx++) {
			vector<ListChildPiece> entry_pieces;
			for (auto &piece : pieces) {
				entry_pieces.emplace_back(StructVector::GetEntries(piece.vector)[entry_idx], piece.offset, piece.count);
			}
			ConcatenateListChildren(entry_pieces, total, result_entries[entry_idx]);
		}
		FlatVector::SetSize(struct_result, total);
		result.Reference(struct_result);
		return;
	}
	if (type.InternalType() == PhysicalType::LIST && AllFlat(pieces)) {
		Vector list_result(type, total);
		CopyValidity(pieces, list_result);
		auto result_data = FlatVector::GetDataMutable<list_entry_t>(list_result);
		vector<ListChildPiece> child_pieces;
		idx_t child_total = 0;
		idx_t result_idx = 0;
		for (auto &piece : pieces) {
			auto piece_data = FlatVector::GetData<list_entry_t>(piece.vector);
			// the list reader appends child values in row order, so the non-empty lists of a piece cover one range
			idx_t child_start = NumericLimits<idx_t>::Maximum();
			idx_t child_end = 0;
			for (idx_t i = 0; i < piece.count; i++) {
				auto &entry = piece_data[piece.offset + i];
				if (entry.length > 0) {
					child_start = MinValue<idx_t>(child_start, entry.offset);
					child_end = MaxValue<idx_t>(child_end, entry.offset + entry.length);
				}
			}
			for (idx_t i = 0; i < piece.count; i++, result_idx++) {
				auto &entry = piece_data[piece.offset + i];
				result_data[result_idx].offset = entry.length > 0 ? child_total + entry.offset - child_start : 0;
				result_data[result_idx].length = entry.length;
			}
			if (child_end > child_start) {
				child_pieces.emplace_back(ListVector::GetChild(piece.vector), child_start, child_end - child_start);
				child_total += child_end - child_start;
			}
		}
		FlatVector::SetSize(list_result, total);
		if (child_total > 0) {
			ConcatenateListChildren(child_pieces, child_total, ListVector::GetChildMutable(list_result));
		}
		ListVector::SetListSize(list_result, child_total);
		result.Reference(list_result);
		return;
	}
	Vector flat_result(type, total);
	idx_t result_idx = 0;
	for (auto &piece : pieces) {
		VectorOperations::Copy(piece.vector, flat_result, piece.offset + piece.count, piece.offset, result_idx);
		result_idx += piece.count;
	}
	FlatVector::SetSize(flat_result, total);
	result.Reference(flat_result);
}

struct ListReaderData {
	ListReaderData(list_entry_t *result_ptr, ValidityMask &result_mask)
	    : result_ptr(result_ptr), result_mask(result_mask) {
	}

	list_entry_t *result_ptr;
	ValidityMask &result_mask;
	vector<ListChildPiece> child_pieces;
	idx_t child_count = 0;
};

struct TemplatedListReader {
	using DATA = ListReaderData;

	static DATA Initialize(optional_ptr<Vector> result_out) {
		D_ASSERT(ListVector::GetListSize(*result_out) == 0);

		auto result_ptr = FlatVector::GetDataMutable<list_entry_t>(*result_out);
		auto &result_mask = FlatVector::ValidityMutable(*result_out);
		return ListReaderData(result_ptr, result_mask);
	}

	static idx_t GetOffset(DATA &data) {
		return data.child_count;
	}

	static void HandleRepeat(DATA &data, idx_t offset) {
		data.result_ptr[offset].length++;
	}

	static void HandleListStart(DATA &data, idx_t offset, idx_t offset_in_child, idx_t length) {
		data.result_ptr[offset].offset = offset_in_child;
		data.result_ptr[offset].length = length;
	}

	static void HandleNull(DATA &data, idx_t offset) {
		data.result_mask.SetInvalid(offset);
		data.result_ptr[offset].offset = 0;
		data.result_ptr[offset].length = 0;
	}

	static void AppendVector(DATA &data, const Vector &read_vector, idx_t offset, idx_t count) {
		if (count == 0) {
			return;
		}
		data.child_pieces.emplace_back(read_vector, offset, count);
		data.child_count += count;
	}

	static void Finalize(optional_ptr<Vector> result_out, DATA &data) {
		if (data.child_count > 0) {
			ConcatenateListChildren(data.child_pieces, data.child_count, ListVector::GetChildMutable(*result_out));
		}
		ListVector::SetListSize(*result_out, data.child_count);
	}
};

struct TemplatedListSkipper {
	using DATA = bool;

	static DATA Initialize(optional_ptr<Vector>) {
		return false;
	}

	static idx_t GetOffset(DATA &) {
		return 0;
	}

	static void HandleRepeat(DATA &, idx_t) {
	}

	static void HandleListStart(DATA &, idx_t, idx_t, idx_t) {
	}

	static void HandleNull(DATA &, idx_t) {
	}

	static void AppendVector(DATA &, const Vector &, idx_t, idx_t) {
	}

	static void Finalize(optional_ptr<Vector>, DATA &) {
	}
};

template <class OP>
idx_t ListColumnReader::ReadInternal(ColumnReaderInput &input, optional_ptr<Vector> result_out) {
	idx_t result_offset = 0;
	auto data = OP::Initialize(result_out);

	auto &num_values = input.num_values;
	auto &define_out = input.define_out;
	auto &repeat_out = input.repeat_out;

	// if an individual list is longer than STANDARD_VECTOR_SIZE we actually have to loop the child read to fill it
	bool finished = false;
	while (!finished) {
		idx_t child_actual_num_values = 0;
		idx_t read_offset = 0;

		// check if we have any overflow from a previous read
		if (overflow_child_count == 0) {
			// we don't: read elements from the child reader
			child_defines.zero();
			child_repeats.zero();
			// we don't know in advance how many values to read because of the beautiful repetition/definition setup
			// we just read (up to) a vector from the child column, and see if we have read enough
			// if we have not read enough, we read another vector
			// if we have read enough, we leave any unhandled elements in the overflow vector for a subsequent read
			auto child_req_num_values =
			    MinValue<idx_t>(STANDARD_VECTOR_SIZE, child_column_reader->GroupRowsAvailable());
			read_vector = make_uniq<Vector>(ListType::GetChildType(Type()));

			ColumnReaderInput child_input(child_req_num_values, child_defines_ptr, child_repeats_ptr);
			child_actual_num_values = child_column_reader->Read(child_input, *read_vector);
			CopyDictionarySelections(*read_vector, child_actual_num_values);
		} else {
			// we do: use the overflow values
			child_actual_num_values = overflow_child_count;
			read_offset = overflow_offset;
			overflow_child_count = 0;
		}

		if (child_actual_num_values == 0) {
			// no more elements available: we are done
			break;
		}
		read_vector->Verify();
		idx_t current_chunk_offset = OP::GetOffset(data);

		// hard-won piece of code this, modify at your own risk
		// the intuition is that we have to only collapse values into lists that are repeated *on this level*
		// the rest is pretty much handed up as-is as a single-valued list or NULL
		idx_t child_idx;
		for (child_idx = 0; child_idx < child_actual_num_values; child_idx++) {
			if (child_repeats_ptr[child_idx] == MaxRepeat()) {
				// value repeats on this level, append
				if (result_offset == 0) {
					throw DataCorruptionException(
					    "Corrupt Parquet file: a value in list column \"%s\" repeats a list before any list started",
					    Schema().name);
				}
				OP::HandleRepeat(data, result_offset - 1);
				continue;
			}

			if (result_offset >= num_values) {
				// we ran out of output space
				finished = true;
				break;
			}
			if (child_defines_ptr[child_idx] >= MaxDefine()) {
				// value has been defined down the stack, hence its NOT NULL
				OP::HandleListStart(data, result_offset, child_idx + current_chunk_offset, 1);
			} else if (child_defines_ptr[child_idx] == MaxDefine() - 1) {
				// empty list
				OP::HandleListStart(data, result_offset, child_idx + current_chunk_offset, 0);
			} else {
				// value is NULL somewhere up the stack
				OP::HandleNull(data, result_offset);
			}

			if (repeat_out) {
				repeat_out[result_offset] = child_repeats_ptr[child_idx];
			}
			if (define_out) {
				define_out[result_offset] = child_defines_ptr[child_idx];
			}

			result_offset++;
		}
		// actually append the required elements to the child list
		OP::AppendVector(data, *read_vector, read_offset, child_idx);

		// we have read more values from the child reader than we can fit into the result for this read
		// we have to pass everything from child_idx to child_actual_num_values into the next call
		if (child_idx < child_actual_num_values && result_offset == num_values) {
			overflow_offset = read_offset + child_idx;
			overflow_child_count = child_actual_num_values - child_idx;

			// move values in the child repeats and defines *backward* by child_idx
			for (idx_t repdef_idx = 0; repdef_idx < overflow_child_count; repdef_idx++) {
				child_defines_ptr[repdef_idx] = child_defines_ptr[child_idx + repdef_idx];
				child_repeats_ptr[repdef_idx] = child_repeats_ptr[child_idx + repdef_idx];
			}
		}
	}
	OP::Finalize(result_out, data);
	return result_offset;
}

idx_t ListColumnReader::Read(ColumnReaderInput &input, Vector &result) {
	ApplyPendingSkips(input.define_out, input.repeat_out);
	return ReadInternal<TemplatedListReader>(input, result);
}

ListColumnReader::ListColumnReader(const ParquetReader &reader, const ParquetColumnSchema &schema,
                                   unique_ptr<ColumnReader> child_column_reader_p)
    : ColumnReader(reader, schema), child_column_reader(std::move(child_column_reader_p)), overflow_child_count(0) {
	child_defines.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_repeats.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_defines_ptr = (uint8_t *)child_defines.ptr;
	child_repeats_ptr = (uint8_t *)child_repeats.ptr;
	if (child_column_reader) {
		child_column_reader->SetParent(*this);
	}
}

ColumnReader &ListColumnReader::GetChildReader() {
	return *child_column_reader;
}

void ListColumnReader::ApplyPendingSkips(data_ptr_t define_out, data_ptr_t repeat_out) {
	ColumnReaderInput empty_input(pending_skips, nullptr, nullptr);
	ReadInternal<TemplatedListSkipper>(empty_input, nullptr);
	pending_skips = 0;
}

} // namespace duckdb
