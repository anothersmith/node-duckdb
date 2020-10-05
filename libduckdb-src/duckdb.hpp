// See https://raw.githubusercontent.com/cwida/duckdb/master/LICENSE for licensing information

#pragma once
#define DUCKDB_AMALGAMATION 1
#define DUCKDB_SOURCE_ID "b'd9bceddc7'"
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/connection.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/materialized_query_result.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/chunk_collection.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/order_type.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/constants.hpp
//
//
//===----------------------------------------------------------------------===//



#include <memory>
#include <cstdint>
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/string.hpp
//
//
//===----------------------------------------------------------------------===//



#include <string>
#include <sstream>

namespace duckdb {
using std::string;
}


namespace duckdb {

//! inline std directives that we use frequently
using std::move;
using std::shared_ptr;
using std::unique_ptr;
using data_ptr = unique_ptr<char[]>;
using std::make_shared;

// NOTE: there is a copy of this in the Postgres' parser grammar (gram.y)
#define DEFAULT_SCHEMA "main"
#define TEMP_SCHEMA "temp"
#define INVALID_SCHEMA ""

//! a saner size_t for loop indices etc
typedef uint64_t idx_t;

//! The type used for row identifiers
typedef int64_t row_t;

//! The type used for hashes
typedef uint64_t hash_t;

//! The value used to signify an invalid index entry
extern const idx_t INVALID_INDEX;

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

//! Type used to represent dates
typedef int32_t date_t;
//! Type used to represent time
typedef int32_t dtime_t;
//! Type used to represent timestamps
typedef int64_t timestamp_t;
//! Type used for the selection vector
typedef uint16_t sel_t;
//! Type used for transaction timestamps
typedef idx_t transaction_t;

//! Type used for column identifiers
typedef idx_t column_t;
//! Special value used to signify the ROW ID of a table
extern const column_t COLUMN_IDENTIFIER_ROW_ID;

//! The maximum row identifier used in tables
extern const row_t MAX_ROW_ID;

extern const transaction_t TRANSACTION_ID_START;
extern const transaction_t MAXIMUM_QUERY_ID;
extern const transaction_t NOT_DELETED_ID;

extern const double PI;

struct Storage {
	//! The size of a hard disk sector, only really needed for Direct IO
	constexpr static int SECTOR_SIZE = 4096;
	//! Block header size for blocks written to the storage
	constexpr static int BLOCK_HEADER_SIZE = sizeof(uint64_t);
	// Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. We
	// default to 256KB. (1 << 18)
	constexpr static int BLOCK_ALLOC_SIZE = 262144;
	//! The actual memory space that is available within the blocks
	constexpr static int BLOCK_SIZE = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;
	//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default
	//! to the page size, which is 4KB. (1 << 12)
	constexpr static int FILE_HEADER_SIZE = 4096;
};

uint64_t NextPowerOfTwo(uint64_t v);

} // namespace duckdb


namespace duckdb {

enum class OrderType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, ASCENDING = 2, DESCENDING = 3 };
enum class OrderByNullType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, NULLS_FIRST = 2, NULLS_LAST = 3 };

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/common.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/helper.hpp
//
//
//===----------------------------------------------------------------------===//





#ifdef _MSC_VER
#define suint64_t int64_t
#endif

namespace duckdb {
#if !defined(_MSC_VER) && (__cplusplus < 201402L)
template <typename T, typename... Args> unique_ptr<T> make_unique(Args &&... args) {
	return unique_ptr<T>(new T(std::forward<Args>(args)...));
}
#else // Visual Studio has make_unique
using std::make_unique;
#endif
template <typename S, typename T, typename... Args> unique_ptr<S> make_unique_base(Args &&... args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}

template <typename T, typename S> unique_ptr<S> unique_ptr_cast(unique_ptr<T> src) {
	return unique_ptr<S>(static_cast<S *>(src.release()));
}

template<typename T>
T MaxValue(T a, T b) {
	return a > b ? a : b;
}

template<typename T>
T MinValue(T a, T b) {
	return a < b ? a : b;
}


} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitset.hpp
//
//
//===----------------------------------------------------------------------===//



#include <bitset>

namespace duckdb {
using std::bitset;
}


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/assert.hpp
//
//
//===----------------------------------------------------------------------===//



#include <assert.h>


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector.hpp
//
//
//===----------------------------------------------------------------------===//



#include <vector>

namespace duckdb {
using std::vector;
}


namespace duckdb {

class Serializer;
class Deserializer;

struct blob_t {
	data_ptr_t data;
	idx_t size;
};

struct interval_t {
	int32_t months;
	int32_t days;
	int64_t msecs;
};

struct hugeint_t {
public:
	uint64_t lower;
	int64_t upper;

public:
	hugeint_t() = default;
	hugeint_t(int64_t value);
	hugeint_t(const hugeint_t &rhs) = default;
	hugeint_t(hugeint_t &&rhs) = default;
	hugeint_t &operator=(const hugeint_t &rhs) = default;
	hugeint_t &operator=(hugeint_t &&rhs) = default;

	string ToString() const;

	// comparison operators
	bool operator==(const hugeint_t &rhs) const;
	bool operator!=(const hugeint_t &rhs) const;
	bool operator<=(const hugeint_t &rhs) const;
	bool operator<(const hugeint_t &rhs) const;
	bool operator>(const hugeint_t &rhs) const;
	bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	hugeint_t operator+(const hugeint_t &rhs) const;
	hugeint_t operator-(const hugeint_t &rhs) const;
	hugeint_t operator*(const hugeint_t &rhs) const;
	hugeint_t operator/(const hugeint_t &rhs) const;
	hugeint_t operator%(const hugeint_t &rhs) const;
	hugeint_t operator-() const;

	// bitwise operators
	hugeint_t operator>>(const hugeint_t &rhs) const;
	hugeint_t operator<<(const hugeint_t &rhs) const;
	hugeint_t operator&(const hugeint_t &rhs) const;
	hugeint_t operator|(const hugeint_t &rhs) const;
	hugeint_t operator^(const hugeint_t &rhs) const;
	hugeint_t operator~() const;

	// in-place operators
	hugeint_t &operator+=(const hugeint_t &rhs);
	hugeint_t &operator-=(const hugeint_t &rhs);
	hugeint_t &operator*=(const hugeint_t &rhs);
	hugeint_t &operator/=(const hugeint_t &rhs);
	hugeint_t &operator%=(const hugeint_t &rhs);
	hugeint_t &operator>>=(const hugeint_t &rhs);
	hugeint_t &operator<<=(const hugeint_t &rhs);
	hugeint_t &operator&=(const hugeint_t &rhs);
	hugeint_t &operator|=(const hugeint_t &rhs);
	hugeint_t &operator^=(const hugeint_t &rhs);
};

struct string_t;

template <class T> using child_list_t = std::vector<std::pair<std::string, T>>;
template <class T> using buffer_ptr = std::shared_ptr<T>;

template <class T, typename... Args> buffer_ptr<T> make_buffer(Args &&... args) {
	return std::make_shared<T>(std::forward<Args>(args)...);
}

struct list_entry_t {
	list_entry_t() = default;
	list_entry_t(uint64_t offset, uint64_t length) : offset(offset), length(length) {
	}

	uint64_t offset;
	uint64_t length;
};

//===--------------------------------------------------------------------===//
// Internal Types
//===--------------------------------------------------------------------===//

// taken from arrow's type.h
enum class PhysicalType : uint8_t {
	/// A NULL type having no physical storage
	NA = 0,

	/// Boolean as 1 bit, LSB bit-packed ordering
	BOOL = 1,

	/// Unsigned 8-bit little-endian integer
	UINT8 = 2,

	/// Signed 8-bit little-endian integer
	INT8 = 3,

	/// Unsigned 16-bit little-endian integer
	UINT16 = 4,

	/// Signed 16-bit little-endian integer
	INT16 = 5,

	/// Unsigned 32-bit little-endian integer
	UINT32 = 6,

	/// Signed 32-bit little-endian integer
	INT32 = 7,

	/// Unsigned 64-bit little-endian integer
	UINT64 = 8,

	/// Signed 64-bit little-endian integer
	INT64 = 9,

	/// 2-byte floating point value
	HALF_FLOAT = 10,

	/// 4-byte floating point value
	FLOAT = 11,

	/// 8-byte floating point value
	DOUBLE = 12,

	/// UTF8 variable-length string as List<Char>
	STRING = 13,

	/// Variable-length bytes (no guarantee of UTF8-ness)
	BINARY = 14,

	/// Fixed-size binary. Each value occupies the same number of bytes
	FIXED_SIZE_BINARY = 15,

	/// int32_t days since the UNIX epoch
	DATE32 = 16,

	/// int64_t milliseconds since the UNIX epoch
	DATE64 = 17,

	/// Exact timestamp encoded with int64 since UNIX epoch
	/// Default unit millisecond
	TIMESTAMP = 18,

	/// Time as signed 32-bit integer, representing either seconds or
	/// milliseconds since midnight
	TIME32 = 19,

	/// Time as signed 64-bit integer, representing either microseconds or
	/// nanoseconds since midnight
	TIME64 = 20,

	/// YEAR_MONTH or DAY_TIME interval in SQL style
	INTERVAL = 21,

	/// Precision- and scale-based decimal type. Storage type depends on the
	/// parameters.
	// DECIMAL = 22,

	/// A list of some logical data type
	LIST = 23,

	/// Struct of logical types
	STRUCT = 24,

	/// Unions of logical types
	UNION = 25,

	/// Dictionary-encoded type, also called "categorical" or "factor"
	/// in other programming languages. Holds the dictionary value
	/// type but not the dictionary itself, which is part of the
	/// ArrayData struct
	DICTIONARY = 26,

	/// Map, a repeated struct logical type
	MAP = 27,

	/// Custom data type, implemented by user
	EXTENSION = 28,

	/// Fixed size list of some logical type
	FIXED_SIZE_LIST = 29,

	/// Measure of elapsed time in either seconds, milliseconds, microseconds
	/// or nanoseconds.
	DURATION = 30,

	/// Like STRING, but with 64-bit offsets
	LARGE_STRING = 31,

	/// Like BINARY, but with 64-bit offsets
	LARGE_BINARY = 32,

	/// Like LIST, but with 64-bit offsets
	LARGE_LIST = 33,

	// DuckDB Extensions
	VARCHAR = 200, // our own string representation, different from STRING and LARGE_STRING above
	VARBINARY = 201,
	POINTER = 202,
	HASH = 203,
	INT128 = 204, // 128-bit integers

	INVALID = 255
};

//===--------------------------------------------------------------------===//
// SQL Types
//===--------------------------------------------------------------------===//
enum class LogicalTypeId : uint8_t {
	INVALID = 0,
	SQLNULL = 1, /* NULL type, used for constant NULL */
	UNKNOWN = 2, /* unknown type, used for parameter expressions */
	ANY = 3,     /* ANY type, used for functions that accept any type as parameter */

	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
	INTEGER = 13,
	BIGINT = 14,
	DATE = 15,
	TIME = 16,
	TIMESTAMP = 17,
	DECIMAL = 18,
	FLOAT = 19,
	DOUBLE = 20,
	CHAR = 21,
	VARCHAR = 22,
	VARBINARY = 23,
	BLOB = 24,
	INTERVAL = 25,

	HUGEINT = 50,
	POINTER = 51,
	HASH = 52,

	STRUCT = 100,
	LIST = 101
};

struct LogicalType {
	LogicalType();
	LogicalType(LogicalTypeId id);
	LogicalType(LogicalTypeId id, string collation);
	LogicalType(LogicalTypeId id, uint8_t width, uint8_t scale);
	LogicalType(LogicalTypeId id, child_list_t<LogicalType> child_types);
	LogicalType(LogicalTypeId id, uint8_t width, uint8_t scale, string collation,
	            child_list_t<LogicalType> child_types);

	LogicalTypeId id() const {
		return id_;
	}
	uint8_t width() const {
		return width_;
	}
	uint8_t scale() const {
		return scale_;
	}
	const string &collation() const {
		return collation_;
	}
	const child_list_t<LogicalType> &child_types() const {
		return child_types_;
	}
	PhysicalType InternalType() const {
		return physical_type_;
	}

	bool operator==(const LogicalType &rhs) const {
		return id_ == rhs.id_ && width_ == rhs.width_ && scale_ == rhs.scale_;
	}
	bool operator!=(const LogicalType &rhs) const {
		return !(*this == rhs);
	}

	//! Serializes a LogicalType to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a blob back into an LogicalType
	static LogicalType Deserialize(Deserializer &source);

	string ToString() const;
	bool IsIntegral() const;
	bool IsNumeric() const;
	bool IsMoreGenericThan(LogicalType &other) const;
	hash_t Hash() const;

	static LogicalType MaxLogicalType(LogicalType left, LogicalType right);

	//! Gets the decimal properties of a numeric type. Fails if the type is not numeric.
	bool GetDecimalProperties(int &width, int &scale) const;

	void Verify() const;

private:
	LogicalTypeId id_;
	uint8_t width_;
	uint8_t scale_;
	string collation_;

	child_list_t<LogicalType> child_types_;
	PhysicalType physical_type_;

private:
	PhysicalType GetInternalType();

public:
	static const LogicalType SQLNULL;
	static const LogicalType BOOLEAN;
	static const LogicalType TINYINT;
	static const LogicalType SMALLINT;
	static const LogicalType INTEGER;
	static const LogicalType BIGINT;
	static const LogicalType FLOAT;
	static const LogicalType DOUBLE;
	static const LogicalType DECIMAL;
	static const LogicalType DATE;
	static const LogicalType TIMESTAMP;
	static const LogicalType TIME;
	static const LogicalType VARCHAR;
	static const LogicalType VARBINARY;
	static const LogicalType STRUCT;
	static const LogicalType LIST;
	static const LogicalType ANY;
	static const LogicalType BLOB;
	static const LogicalType INTERVAL;
	static const LogicalType HUGEINT;
	static const LogicalType HASH;
	static const LogicalType POINTER;
	static const LogicalType INVALID;

	//! A list of all NUMERIC types (integral and floating point types)
	static const vector<LogicalType> NUMERIC;
	//! A list of all INTEGRAL types
	static const vector<LogicalType> INTEGRAL;
	//! A list of ALL SQL types
	static const vector<LogicalType> ALL_TYPES;
};

string LogicalTypeIdToString(LogicalTypeId type);

LogicalType TransformStringToLogicalType(string str);

//! Returns the PhysicalType for the given type
template <class T> PhysicalType GetTypeId() {
	if (std::is_same<T, bool>()) {
		return PhysicalType::BOOL;
	} else if (std::is_same<T, int8_t>()) {
		return PhysicalType::INT8;
	} else if (std::is_same<T, int16_t>()) {
		return PhysicalType::INT16;
	} else if (std::is_same<T, int32_t>()) {
		return PhysicalType::INT32;
	} else if (std::is_same<T, int64_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, hugeint_t>()) {
		return PhysicalType::INT128;
	} else if (std::is_same<T, uint64_t>()) {
		return PhysicalType::HASH;
	} else if (std::is_same<T, uintptr_t>()) {
		return PhysicalType::POINTER;
	} else if (std::is_same<T, float>()) {
		return PhysicalType::FLOAT;
	} else if (std::is_same<T, double>()) {
		return PhysicalType::DOUBLE;
	} else if (std::is_same<T, const char *>() || std::is_same<T, char *>()) {
		return PhysicalType::VARCHAR;
	} else if (std::is_same<T, interval_t>()) {
		return PhysicalType::INTERVAL;
	} else {
		return PhysicalType::INVALID;
	}
}

template <class T> bool IsValidType() {
	return GetTypeId<T>() != PhysicalType::INVALID;
}

//! The PhysicalType used by the row identifiers column
extern const LogicalType LOGICAL_ROW_TYPE;
extern const PhysicalType ROW_TYPE;

string TypeIdToString(PhysicalType type);
idx_t GetTypeIdSize(PhysicalType type);
bool TypeIsConstantSize(PhysicalType type);
bool TypeIsIntegral(PhysicalType type);
bool TypeIsNumeric(PhysicalType type);
bool TypeIsInteger(PhysicalType type);

template <class T> bool IsIntegerType() {
	return TypeIsIntegral(GetTypeId<T>());
}

bool ApproxEqual(float l, float r);
bool ApproxEqual(double l, double r);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_size.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! The vector size used in the execution engine
#ifndef STANDARD_VECTOR_SIZE
#define STANDARD_VECTOR_SIZE 1024
#endif

#if ((STANDARD_VECTOR_SIZE & (STANDARD_VECTOR_SIZE - 1)) != 0)
#error Vector size should be a power of two
#endif

//! Zero selection vector: completely filled with the value 0 [READ ONLY]
extern const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];

}


namespace duckdb {
class VectorBuffer;

struct SelectionData {
	SelectionData(idx_t count) {
		owned_data = unique_ptr<sel_t[]>(new sel_t[count]);
	}

	unique_ptr<sel_t[]> owned_data;
};

struct SelectionVector {
	SelectionVector() : sel_vector(nullptr) {
	}
	SelectionVector(sel_t *sel) {
		Initialize(sel);
	}
	SelectionVector(idx_t count) {
		Initialize(count);
	}
	SelectionVector(const SelectionVector &sel_vector) {
		Initialize(sel_vector);
	}
	SelectionVector(buffer_ptr<SelectionData> data) {
		Initialize(move(data));
	}

public:
	void Initialize(sel_t *sel) {
		selection_data.reset();
		sel_vector = sel;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		selection_data = make_buffer<SelectionData>(count);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(buffer_ptr<SelectionData> data) {
		selection_data = move(data);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(const SelectionVector &other) {
		selection_data = other.selection_data;
		sel_vector = other.sel_vector;
	}

	bool empty() const {
		return !sel_vector;
	}
	void set_index(idx_t idx, idx_t loc) {
		sel_vector[idx] = loc;
	}
	void swap(idx_t i, idx_t j) {
		sel_t tmp = sel_vector[i];
		sel_vector[i] = sel_vector[j];
		sel_vector[j] = tmp;
	}
	idx_t get_index(idx_t idx) const {
		return sel_vector[idx];
	}
	sel_t *data() {
		return sel_vector;
	}
	buffer_ptr<SelectionData> sel_data() {
		return selection_data;
	}
	buffer_ptr<SelectionData> Slice(const SelectionVector &sel, idx_t count);

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

private:
	sel_t *sel_vector;
	buffer_ptr<SelectionData> selection_data;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/value.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception.hpp
//
//
//===----------------------------------------------------------------------===//







#include <stdexcept>

namespace duckdb {
enum class PhysicalType : uint8_t;
struct LogicalType;
struct hugeint_t;

inline void assert_restrict_function(void *left_start, void *left_end, void *right_start, void *right_end,
                                     const char *fname, int linenr) {
	// assert that the two pointers do not overlap
#ifdef DEBUG
	if (!(left_end <= right_start || right_end <= left_start)) {
		printf("ASSERT RESTRICT FAILED: %s:%d\n", fname, linenr);
		assert(0);
	}
#endif
}

#define ASSERT_RESTRICT(left_start, left_end, right_start, right_end)                                                  \
	assert_restrict_function(left_start, left_end, right_start, right_end, __FILE__, __LINE__)

//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//

enum class ExceptionType {
	INVALID = 0,          // invalid type
	OUT_OF_RANGE = 1,     // value out of range error
	CONVERSION = 2,       // conversion/casting error
	UNKNOWN_TYPE = 3,     // unknown type
	DECIMAL = 4,          // decimal related
	MISMATCH_TYPE = 5,    // type mismatch
	DIVIDE_BY_ZERO = 6,   // divide by 0
	OBJECT_SIZE = 7,      // object size exceeded
	INVALID_TYPE = 8,     // incompatible for operation
	SERIALIZATION = 9,    // serialization
	TRANSACTION = 10,     // transaction management
	NOT_IMPLEMENTED = 11, // method not implemented
	EXPRESSION = 12,      // expression parsing
	CATALOG = 13,         // catalog related
	PARSER = 14,          // parser related
	PLANNER = 15,         // planner related
	SCHEDULER = 16,       // scheduler related
	EXECUTOR = 17,        // executor related
	CONSTRAINT = 18,      // constraint related
	INDEX = 19,           // index related
	STAT = 20,            // stat related
	CONNECTION = 21,      // connection related
	SYNTAX = 22,          // syntax related
	SETTINGS = 23,        // settings related
	BINDER = 24,          // binder related
	NETWORK = 25,         // network related
	OPTIMIZER = 26,       // optimizer related
	NULL_POINTER = 27,    // nullptr exception
	IO = 28,              // IO exception
	INTERRUPT = 29,       // interrupt
	FATAL = 30, // Fatal exception: fatal exceptions are non-recoverable, and render the entire DB in an unusable state
	INTERNAL =
	    31, // Internal exception: exception that indicates something went wrong internally (i.e. bug in the code base)
	INVALID_INPUT = 32 // Input or arguments error
};

enum class ExceptionFormatValueType : uint8_t {
	FORMAT_VALUE_TYPE_DOUBLE,
	FORMAT_VALUE_TYPE_INTEGER,
	FORMAT_VALUE_TYPE_STRING
};

struct ExceptionFormatValue {
	ExceptionFormatValue(double dbl_val) : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE), dbl_val(dbl_val) {
	}
	ExceptionFormatValue(int64_t int_val)
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER), int_val(int_val) {
	}
	ExceptionFormatValue(string str_val) : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(str_val) {
	}

	ExceptionFormatValueType type;

	double dbl_val;
	int64_t int_val;
	string str_val;

	template <class T> static ExceptionFormatValue CreateFormatValue(T value) {
		return int64_t(value);
	}
};

template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(LogicalType value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value);

class Exception : public std::exception {
public:
	Exception(string message);
	Exception(ExceptionType exception_type, string message);

	ExceptionType type;

public:
	const char *what() const noexcept override;

	string ExceptionTypeToString(ExceptionType type);

	template <typename... Args> static string ConstructMessage(string msg, Args... params) {
		vector<ExceptionFormatValue> values;
		return ConstructMessageRecursive(msg, values, params...);
	}

	static string ConstructMessageRecursive(string msg, vector<ExceptionFormatValue> &values);

	template <class T, typename... Args>
	static string ConstructMessageRecursive(string msg, vector<ExceptionFormatValue> &values, T param, Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return ConstructMessageRecursive(msg, values, params...);
	}

private:
	string exception_message_;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

//! Exceptions that are StandardExceptions do NOT invalidate the current transaction when thrown
class StandardException : public Exception {
public:
	StandardException(ExceptionType exception_type, string message) : Exception(exception_type, message) {
	}
};

class CatalogException : public StandardException {
public:
	CatalogException(string msg);

	template <typename... Args>
	CatalogException(string msg, Args... params) : CatalogException(ConstructMessage(msg, params...)) {
	}
};

class ParserException : public StandardException {
public:
	ParserException(string msg);

	template <typename... Args>
	ParserException(string msg, Args... params) : ParserException(ConstructMessage(msg, params...)) {
	}
};

class BinderException : public StandardException {
public:
	BinderException(string msg);

	template <typename... Args>
	BinderException(string msg, Args... params) : BinderException(ConstructMessage(msg, params...)) {
	}
};

class ConversionException : public Exception {
public:
	ConversionException(string msg);

	template <typename... Args>
	ConversionException(string msg, Args... params) : ConversionException(ConstructMessage(msg, params...)) {
	}
};

class TransactionException : public Exception {
public:
	TransactionException(string msg);

	template <typename... Args>
	TransactionException(string msg, Args... params) : TransactionException(ConstructMessage(msg, params...)) {
	}
};

class NotImplementedException : public Exception {
public:
	NotImplementedException(string msg);

	template <typename... Args>
	NotImplementedException(string msg, Args... params) : NotImplementedException(ConstructMessage(msg, params...)) {
	}
};

class OutOfRangeException : public Exception {
public:
	OutOfRangeException(string msg);

	template <typename... Args>
	OutOfRangeException(string msg, Args... params) : OutOfRangeException(ConstructMessage(msg, params...)) {
	}
};

class SyntaxException : public Exception {
public:
	SyntaxException(string msg);

	template <typename... Args>
	SyntaxException(string msg, Args... params) : SyntaxException(ConstructMessage(msg, params...)) {
	}
};

class ConstraintException : public Exception {
public:
	ConstraintException(string msg);

	template <typename... Args>
	ConstraintException(string msg, Args... params) : ConstraintException(ConstructMessage(msg, params...)) {
	}
};

class IOException : public Exception {
public:
	IOException(string msg);

	template <typename... Args>
	IOException(string msg, Args... params) : IOException(ConstructMessage(msg, params...)) {
	}
};

class SerializationException : public Exception {
public:
	SerializationException(string msg);

	template <typename... Args>
	SerializationException(string msg, Args... params) : SerializationException(ConstructMessage(msg, params...)) {
	}
};

class SequenceException : public Exception {
public:
	SequenceException(string msg);

	template <typename... Args>
	SequenceException(string msg, Args... params) : SequenceException(ConstructMessage(msg, params...)) {
	}
};

class InterruptException : public Exception {
public:
	InterruptException();
};

class FatalException : public Exception {
public:
	FatalException(string msg);

	template <typename... Args>
	FatalException(string msg, Args... params) : FatalException(ConstructMessage(msg, params...)) {
	}
};

class InternalException : public Exception {
public:
	InternalException(string msg);

	template <typename... Args>
	InternalException(string msg, Args... params) : InternalException(ConstructMessage(msg, params...)) {
	}
};

class InvalidInputException : public Exception {
public:
	InvalidInputException(string msg);

	template <typename... Args>
	InvalidInputException(string msg, Args... params) : InvalidInputException(ConstructMessage(msg, params...)) {
	}
};

class CastException : public Exception {
public:
	CastException(const PhysicalType origType, const PhysicalType newType);
	CastException(const LogicalType origType, const LogicalType newType);
};

class InvalidTypeException : public Exception {
public:
	InvalidTypeException(PhysicalType type, string msg);
	InvalidTypeException(LogicalType type, string msg);
};

class TypeMismatchException : public Exception {
public:
	TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, string msg);
	TypeMismatchException(const LogicalType type_1, const LogicalType type_2, string msg);
};

class ValueOutOfRangeException : public Exception {
public:
	ValueOutOfRangeException(const int64_t value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const hugeint_t value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const double value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const PhysicalType varType, const idx_t length);
};

} // namespace duckdb



namespace duckdb {

class Deserializer;
class Serializer;

//! The Value object holds a single arbitrary value of any type that can be
//! stored in the database.
class Value {
	friend class Vector;

public:
	//! Create an empty NULL value of the specified type
	explicit Value(LogicalType type = LogicalType::SQLNULL) : type_(type), is_null(true) {
	}
	//! Create a BIGINT value
	Value(int32_t val) : type_(LogicalType::INTEGER), is_null(false) {
		value_.integer = val;
	}
	//! Create a BIGINT value
	Value(int64_t val) : type_(LogicalType::BIGINT), is_null(false) {
		value_.bigint = val;
	}
	//! Create a FLOAT value
	Value(float val) : type_(LogicalType::FLOAT), is_null(false) {
		value_.float_ = val;
	}
	//! Create a DOUBLE value
	Value(double val) : type_(LogicalType::DOUBLE), is_null(false) {
		value_.double_ = val;
	}
	//! Create a VARCHAR value
	Value(const char *val) : Value(val ? string(val) : string()) {
	}
	Value(string_t val);
	//! Create a VARCHAR value
	Value(string val);

	LogicalType type() const {
		return type_;
	}

	//! Create the lowest possible value of a given type (numeric only)
	static Value MinimumValue(PhysicalType type);
	//! Create the highest possible value of a given type (numeric only)
	static Value MaximumValue(PhysicalType type);
	//! Create a Numeric value of the specified type with the specified value
	static Value Numeric(LogicalType type, int64_t value);
	static Value Numeric(LogicalType type, hugeint_t value);

	//! Create a tinyint Value from a specified value
	static Value BOOLEAN(int8_t value);
	//! Create a tinyint Value from a specified value
	static Value TINYINT(int8_t value);
	//! Create a smallint Value from a specified value
	static Value SMALLINT(int16_t value);
	//! Create an integer Value from a specified value
	static Value INTEGER(int32_t value);
	//! Create a bigint Value from a specified value
	static Value BIGINT(int64_t value);
	//! Create a hugeint Value from a specified value
	static Value HUGEINT(hugeint_t value);
	//! Create a hash Value from a specified value
	static Value HASH(hash_t value);
	//! Create a pointer Value from a specified value
	static Value POINTER(uintptr_t value);
	//! Create a date Value from a specified date
	static Value DATE(date_t date);
	//! Create a date Value from a specified date
	static Value DATE(int32_t year, int32_t month, int32_t day);
	//! Create a time Value from a specified time
	static Value TIME(dtime_t time);
	//! Create a time Value from a specified time
	static Value TIME(int32_t hour, int32_t min, int32_t sec, int32_t msec);
	//! Create a timestamp Value from a specified date/time combination
	static Value TIMESTAMP(date_t date, dtime_t time);
	//! Create a timestamp Value from a specified timestamp
	static Value TIMESTAMP(timestamp_t timestamp);
	//! Create a timestamp Value from a specified timestamp in separate values
	static Value TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
	                       int32_t msec);
	static Value INTERVAL(int32_t months, int32_t days, int64_t msecs);
	static Value INTERVAL(interval_t interval);

	// Decimal values
	static Value DECIMAL(int16_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(int32_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(int64_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(hugeint_t value, uint8_t width, uint8_t scale);
	//! Create a float Value from a specified value
	static Value FLOAT(float value);
	//! Create a double Value from a specified value
	static Value DOUBLE(double value);
	//! Create a struct value with given list of entries
	static Value STRUCT(child_list_t<Value> values);
	//! Create a list value with the given entries
	static Value LIST(std::vector<Value> values);

	//! Create a blob Value from a specified value
	static Value BLOB(string data, bool must_cast = true);

	template <class T> T GetValue() const {
		throw NotImplementedException("Unimplemented template type for Value::GetValue");
	}
	template <class T> static Value CreateValue(T value) {
		throw NotImplementedException("Unimplemented template type for Value::CreateValue");
	}

	//! Return a copy of this value
	Value Copy() const {
		return Value(*this);
	}

	//! Convert this value to a string
	string ToString() const;

	//! Cast this value to another type
	Value CastAs(LogicalType target_type, bool strict = false) const;
	//! Tries to cast value to another type, throws exception if its not possible
	bool TryCastAs(LogicalType target_type, bool strict = false);

	//! Serializes a Value to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a Value from a blob
	static Value Deserialize(Deserializer &source);

	//===--------------------------------------------------------------------===//
	// Numeric Operators
	//===--------------------------------------------------------------------===//
	Value operator+(const Value &rhs) const;
	Value operator-(const Value &rhs) const;
	Value operator*(const Value &rhs) const;
	Value operator/(const Value &rhs) const;
	Value operator%(const Value &rhs) const;

	//===--------------------------------------------------------------------===//
	// Comparison Operators
	//===--------------------------------------------------------------------===//
	bool operator==(const Value &rhs) const;
	bool operator!=(const Value &rhs) const;
	bool operator<(const Value &rhs) const;
	bool operator>(const Value &rhs) const;
	bool operator<=(const Value &rhs) const;
	bool operator>=(const Value &rhs) const;

	bool operator==(const int64_t &rhs) const;
	bool operator!=(const int64_t &rhs) const;
	bool operator<(const int64_t &rhs) const;
	bool operator>(const int64_t &rhs) const;
	bool operator<=(const int64_t &rhs) const;
	bool operator>=(const int64_t &rhs) const;

	static bool FloatIsValid(float value);
	static bool DoubleIsValid(double value);
	//! Returns true if the values are (approximately) equivalent. Note this is NOT the SQL equivalence. For this
	//! function, NULL values are equivalent and floating point values that are close are equivalent.
	static bool ValuesAreEqual(Value result_value, Value value);

	friend std::ostream &operator<<(std::ostream &out, const Value &val) {
		out << val.ToString();
		return out;
	}
	void Print();

private:
	//! The logical of the value
	LogicalType type_;

public:
	//! Whether or not the value is NULL
	bool is_null;

	//! The value of the object, if it is of a constant size Type
	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		hugeint_t hugeint;
		float float_;
		double double_;
		uintptr_t pointer;
		uint64_t hash;
		interval_t interval;
	} value_;

	//! The value of the object, if it is of a variable size type
	string str_value;

	child_list_t<Value> struct_value;
	std::vector<Value> list_value;

private:
	template <class T> T GetValueInternal() const;
	//! Templated helper function for casting
	template <class DST, class OP> static DST _cast(const Value &v);

	//! Templated helper function for binary operations
	template <class OP>
	static void _templated_binary_operation(const Value &left, const Value &right, Value &result, bool ignore_null);

	//! Templated helper function for boolean operations
	template <class OP> static bool _templated_boolean_operation(const Value &left, const Value &right);
};

template <> Value Value::CreateValue(bool value);
template <> Value Value::CreateValue(int8_t value);
template <> Value Value::CreateValue(int16_t value);
template <> Value Value::CreateValue(int32_t value);
template <> Value Value::CreateValue(int64_t value);
template <> Value Value::CreateValue(hugeint_t value);
template <> Value Value::CreateValue(const char *value);
template <> Value Value::CreateValue(string value);
template <> Value Value::CreateValue(string_t value);
template <> Value Value::CreateValue(float value);
template <> Value Value::CreateValue(double value);
template <> Value Value::CreateValue(Value value);

template <> bool Value::GetValue() const;
template <> int8_t Value::GetValue() const;
template <> int16_t Value::GetValue() const;
template <> int32_t Value::GetValue() const;
template <> int64_t Value::GetValue() const;
template <> hugeint_t Value::GetValue() const;
template <> string Value::GetValue() const;
template <> float Value::GetValue() const;
template <> double Value::GetValue() const;
template <> uintptr_t Value::GetValue() const;


} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/vector_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

enum class VectorType : uint8_t {
	FLAT_VECTOR,       // Flat vectors represent a standard uncompressed vector
	CONSTANT_VECTOR,   // Constant vector represents a single constant
	DICTIONARY_VECTOR, // Dictionary vector represents a selection vector on top of another vector
	SEQUENCE_VECTOR    // Sequence vector represents a sequence with a start point and an increment
};

string VectorTypeToString(VectorType type);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_buffer.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_heap.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
//! A string heap is the owner of a set of strings, strings can be inserted into
//! it On every insert, a pointer to the inserted string is returned The
//! returned pointer will remain valid until the StringHeap is destroyed
class StringHeap {
public:
	StringHeap();

	void Destroy() {
		tail = nullptr;
		chunk = nullptr;
	}

	void Move(StringHeap &other) {
		assert(!other.chunk);
		other.tail = tail;
		other.chunk = move(chunk);
		tail = nullptr;
	}

	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const char *data, idx_t len);
	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const char *data);
	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const string &data);
	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const string_t &data);
	//! Add a blob to the string heap; blobs can be non-valid UTF8
	string_t AddBlob(const char *data, idx_t len);
	//! Allocates space for an empty string of size "len" on the heap
	string_t EmptyString(idx_t len);
	//! Add all strings from a different string heap to this string heap
	void MergeHeap(StringHeap &heap);

private:
	struct StringChunk {
		StringChunk(idx_t size) : current_position(0), maximum_size(size) {
			data = unique_ptr<char[]>(new char[maximum_size]);
		}
		~StringChunk() {
			if (prev) {
				auto current_prev = move(prev);
				while (current_prev) {
					current_prev = move(current_prev->prev);
				}
			}
		}

		unique_ptr<char[]> data;
		idx_t current_position;
		idx_t maximum_size;
		unique_ptr<StringChunk> prev;
	};
	StringChunk *tail;
	unique_ptr<StringChunk> chunk;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_type.hpp
//
//
//===----------------------------------------------------------------------===//




#include <cstring>
#include <cassert>

namespace duckdb {

struct string_t {
	friend struct StringComparisonOperators;
	friend class StringSegment;

public:
	static constexpr idx_t PREFIX_LENGTH = 4 * sizeof(char);
	static constexpr idx_t INLINE_LENGTH = 12;

	string_t() = default;
	string_t(uint32_t len) : length(len) {
		memset(prefix, 0, PREFIX_LENGTH);
		value_.data = nullptr;
	}
	string_t(const char *data, uint32_t len) : length(len) {
		assert(data || length == 0);
		if (IsInlined()) {
			// zero initialize the prefix first
			// this makes sure that strings with length smaller than 4 still have an equal prefix
			memset(prefix, 0, PREFIX_LENGTH);
			if (length == 0) {
				return;
			}
			// small string: inlined
			/* Note: this appears to write out-of bounds on `prefix` if `length` > `PREFIX_LENGTH`
			 but this is not the case because the `value_` union `inlined` char array directly
			 follows it with 8 more chars to use for the string value.
			 */
			memcpy(prefix, data, length);
			prefix[length] = '\0';
		} else {
			// large string: store pointer
			memcpy(prefix, data, PREFIX_LENGTH);
			value_.data = (char *)data;
		}
	}
	string_t(const char *data) : string_t(data, strlen(data)) {
	}
	string_t(const string &value) : string_t(value.c_str(), value.size()) {
	}

	bool IsInlined() const {
		return length < INLINE_LENGTH;
	}

	char *GetData() {
		return IsInlined() ? (char *)prefix : value_.data;
	}

	const char *GetData() const {
		return IsInlined() ? (const char *)prefix : value_.data;
	}

	const char *GetPrefix() const {
		return prefix;
	}

	idx_t GetSize() const {
		return length;
	}

	string GetString() const {
		return string(GetData(), GetSize());
	}

	void Finalize() {
		// set trailing NULL byte
		auto dataptr = (char *)GetData();
		dataptr[length] = '\0';
		if (length < INLINE_LENGTH) {
			// fill prefix with zeros if the length is smaller than the prefix length
			for (idx_t i = length; i < PREFIX_LENGTH; i++) {
				prefix[i] = '\0';
			}
		} else {
			// copy the data into the prefix
			memcpy(prefix, dataptr, PREFIX_LENGTH);
		}
	}

	void Verify();

private:
	uint32_t length;
	char prefix[4];
	union {
		char inlined[8];
		char *data;
	} value_;
};

} // namespace duckdb



namespace duckdb {

class VectorBuffer;
class Vector;
class ChunkCollection;

enum class VectorBufferType : uint8_t {
	STANDARD_BUFFER,     // standard buffer, holds a single array of data
	DICTIONARY_BUFFER,   // dictionary buffer, holds a selection vector
	VECTOR_CHILD_BUFFER, // vector child buffer: holds another vector
	STRING_BUFFER,       // string buffer, holds a string heap
	STRUCT_BUFFER,       // struct buffer, holds a ordered mapping from name to child vector
	LIST_BUFFER          // list buffer, holds a single flatvector child
};

//! The VectorBuffer is a class used by the vector to hold its data
class VectorBuffer {
public:
	VectorBuffer(VectorBufferType type) : type(type) {
	}
	VectorBuffer(idx_t data_size);
	virtual ~VectorBuffer() {
	}

	VectorBufferType type;

public:
	data_ptr_t GetData() {
		return data.get();
	}

	static buffer_ptr<VectorBuffer> CreateStandardVector(PhysicalType type);
	static buffer_ptr<VectorBuffer> CreateConstantVector(PhysicalType type);

protected:
	unique_ptr<data_t[]> data;
};

//! The DictionaryBuffer holds a selection vector
class DictionaryBuffer : public VectorBuffer {
public:
	DictionaryBuffer(const SelectionVector &sel) : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(sel) {
	}
	DictionaryBuffer(buffer_ptr<SelectionData> data)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(move(data)) {
	}
	DictionaryBuffer(idx_t count = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(count) {
	}

public:
	SelectionVector &GetSelVector() {
		return sel_vector;
	}

private:
	SelectionVector sel_vector;
};

class VectorStringBuffer : public VectorBuffer {
public:
	VectorStringBuffer();

public:
	string_t AddString(const char *data, idx_t len) {
		return heap.AddString(data, len);
	}
	string_t AddString(string_t data) {
		return heap.AddString(data);
	}
	string_t AddBlob(string_t data) {
		return heap.AddBlob(data.GetData(), data.GetSize());
	}
	string_t EmptyString(idx_t len) {
		return heap.EmptyString(len);
	}

	void AddHeapReference(buffer_ptr<VectorBuffer> heap) {
		references.push_back(move(heap));
	}

private:
	//! The string heap of this buffer
	StringHeap heap;
	// References to additional vector buffers referenced by this string buffer
	vector<buffer_ptr<VectorBuffer>> references;
};

class VectorStructBuffer : public VectorBuffer {
public:
	VectorStructBuffer();
	~VectorStructBuffer();

public:
	child_list_t<unique_ptr<Vector>> &GetChildren() {
		return children;
	}
	void AddChild(string name, unique_ptr<Vector> vector) {
		children.push_back(std::make_pair(name, move(vector)));
	}

private:
	//! child vectors used for nested data
	child_list_t<unique_ptr<Vector>> children;
};

class VectorListBuffer : public VectorBuffer {
public:
	VectorListBuffer();

	~VectorListBuffer();

public:
	ChunkCollection &GetChild() {
		return *child;
	}
	void SetChild(unique_ptr<ChunkCollection> new_child);

private:
	//! child vectors used for nested data
	unique_ptr<ChunkCollection> child;
};

} // namespace duckdb



namespace duckdb {

//! Type used for nullmasks
typedef bitset<STANDARD_VECTOR_SIZE> nullmask_t;

//! Zero NULL mask: filled with the value 0 [READ ONLY]
extern nullmask_t ZERO_MASK;

struct VectorData {
	const SelectionVector *sel;
	data_ptr_t data;
	nullmask_t *nullmask;
};

class VectorStructBuffer;
class VectorListBuffer;
class ChunkCollection;

struct SelCache;

//!  Vector of values of a specified PhysicalType.
class Vector {
	friend struct ConstantVector;
	friend struct DictionaryVector;
	friend struct FlatVector;
	friend struct ListVector;
	friend struct StringVector;
	friend struct StructVector;
	friend struct SequenceVector;

	friend class DataChunk;

public:
	Vector();
	//! Create a vector of size one holding the passed on value
	Vector(Value value);
	//! Create an empty standard vector with a type, equivalent to calling Vector(type, true, false)
	Vector(LogicalType type);
	//! Create a non-owning vector that references the specified data
	Vector(LogicalType type, data_ptr_t dataptr);
	//! Create an owning vector that holds at most STANDARD_VECTOR_SIZE entries.
	/*!
	    Create a new vector
	    If create_data is true, the vector will be an owning empty vector.
	    If zero_data is true, the allocated data will be zero-initialized.
	*/
	Vector(LogicalType type, bool create_data, bool zero_data);
	// implicit copying of Vectors is not allowed
	Vector(const Vector &) = delete;
	// but moving of vectors is allowed
	Vector(Vector &&other) noexcept;

	//! The vector type specifies how the data of the vector is physically stored (i.e. if it is a single repeated
	//! constant, if it is compressed)
	VectorType vector_type;
	//! The type of the elements stored in the vector (e.g. integer, float)
	LogicalType type;

public:
	//! Create a vector that references the specified value.
	void Reference(const Value &value);
	//! Causes this vector to reference the data held by the other vector.
	void Reference(Vector &other);

	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, idx_t offset);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, const SelectionVector &sel, idx_t count);
	//! Turns the vector into a dictionary vector with the specified dictionary
	void Slice(const SelectionVector &sel, idx_t count);
	//! Slice the vector, keeping the result around in a cache or potentially using the cache instead of slicing
	void Slice(const SelectionVector &sel, idx_t count, SelCache &cache);

	//! Creates the data of this vector with the specified type. Any data that
	//! is currently in the vector is destroyed.
	void Initialize(LogicalType new_type = LogicalType::INVALID, bool zero_data = false);

	//! Converts this Vector to a printable string representation
	string ToString(idx_t count) const;
	void Print(idx_t count);

	string ToString() const;
	void Print();

	//! Flatten the vector, removing any compression and turning it into a FLAT_VECTOR
	void Normalify(idx_t count);
	void Normalify(const SelectionVector &sel, idx_t count);
	//! Obtains a selection vector and data pointer through which the data of this vector can be accessed
	void Orrify(idx_t count, VectorData &data);

	//! Turn the vector into a sequence vector
	void Sequence(int64_t start, int64_t increment);

	//! Verify that the Vector is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	void Verify(idx_t count);
	void Verify(const SelectionVector &sel, idx_t count);
	void UTFVerify(idx_t count);
	void UTFVerify(const SelectionVector &sel, idx_t count);

	//! Returns the [index] element of the Vector as a Value.
	Value GetValue(idx_t index) const;
	//! Sets the [index] element of the Vector to the specified Value.
	void SetValue(idx_t index, Value val);

	//! Serializes a Vector to a stand-alone binary blob
	void Serialize(idx_t count, Serializer &serializer);
	//! Deserializes a blob back into a Vector
	void Deserialize(idx_t count, Deserializer &source);

protected:
	//! A pointer to the data.
	data_ptr_t data;
	//! The nullmask of the vector
	nullmask_t nullmask;
	//! The main buffer holding the data of the vector
	buffer_ptr<VectorBuffer> buffer;
	//! The buffer holding auxiliary data of the vector
	//! e.g. a string vector uses this to store strings
	buffer_ptr<VectorBuffer> auxiliary;
};

//! The DictionaryBuffer holds a selection vector
class VectorChildBuffer : public VectorBuffer {
public:
	VectorChildBuffer() : VectorBuffer(VectorBufferType::VECTOR_CHILD_BUFFER), data() {
	}

public:
	Vector data;
};

struct ConstantVector {
	static inline data_ptr_t GetData(Vector &vector) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR || vector.vector_type == VectorType::FLAT_VECTOR);
		return vector.data;
	}
	template <class T> static inline T *GetData(Vector &vector) {
		return (T *)ConstantVector::GetData(vector);
	}
	static inline bool IsNull(const Vector &vector) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR);
		return vector.nullmask[0];
	}
	static inline void SetNull(Vector &vector, bool is_null) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR);
		vector.nullmask[0] = is_null;
	}
	static inline nullmask_t &Nullmask(Vector &vector) {
		assert(vector.vector_type == VectorType::CONSTANT_VECTOR);
		return vector.nullmask;
	}

	static const sel_t zero_vector[STANDARD_VECTOR_SIZE];
	static const SelectionVector ZeroSelectionVector;
};

struct DictionaryVector {
	static inline SelectionVector &SelVector(const Vector &vector) {
		assert(vector.vector_type == VectorType::DICTIONARY_VECTOR);
		return ((DictionaryBuffer &)*vector.buffer).GetSelVector();
	}
	static inline Vector &Child(const Vector &vector) {
		assert(vector.vector_type == VectorType::DICTIONARY_VECTOR);
		return ((VectorChildBuffer &)*vector.auxiliary).data;
	}
};

struct FlatVector {
	static inline data_ptr_t GetData(Vector &vector) {
		return ConstantVector::GetData(vector);
	}
	template <class T> static inline T *GetData(Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	static inline void SetData(Vector &vector, data_ptr_t data) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		vector.data = data;
	}
	template <class T> static inline T GetValue(Vector &vector, idx_t idx) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		return FlatVector::GetData<T>(vector)[idx];
	}
	static inline nullmask_t &Nullmask(Vector &vector) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		return vector.nullmask;
	}
	static inline void SetNullmask(Vector &vector, nullmask_t new_mask) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		vector.nullmask = move(new_mask);
	}
	static inline void SetNull(Vector &vector, idx_t idx, bool value) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		vector.nullmask[idx] = value;
	}
	static inline bool IsNull(const Vector &vector, idx_t idx) {
		assert(vector.vector_type == VectorType::FLAT_VECTOR);
		return vector.nullmask[idx];
	}

	static const sel_t incremental_vector[STANDARD_VECTOR_SIZE];
	static const SelectionVector IncrementalSelectionVector;
};

struct ListVector {
	static ChunkCollection &GetEntry(const Vector &vector);
	static bool HasEntry(const Vector &vector);
	static void SetEntry(Vector &vector, unique_ptr<ChunkCollection> entry);
};

struct StringVector {
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const char *data, idx_t len);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const char *data);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, string_t data);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const string &data);
	//! Add a string or a blob to the string heap of the vector (auxiliary data)
	//! This function is the same as ::AddString, except the added data does not need to be valid UTF8
	static string_t AddStringOrBlob(Vector &vector, string_t data);
	//! Allocates an empty string of the specified size, and returns a writable pointer that can be used to store the
	//! result of an operation
	static string_t EmptyString(Vector &vector, idx_t len);

	//! Add a reference from this vector to the string heap of the provided vector
	static void AddHeapReference(Vector &vector, Vector &other);
};

struct StructVector {
	static bool HasEntries(const Vector &vector);
	static child_list_t<unique_ptr<Vector>> &GetEntries(const Vector &vector);
	static void AddEntry(Vector &vector, string name, unique_ptr<Vector> entry);
};

struct SequenceVector {
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment) {
		assert(vector.vector_type == VectorType::SEQUENCE_VECTOR);
		auto data = (int64_t *)vector.buffer->GetData();
		start = data[0];
		increment = data[1];
	}
};

} // namespace duckdb


struct ArrowArray;

namespace duckdb {

//!  A Data Chunk represents a set of vectors.
/*!
    The data chunk class is the intermediate representation used by the
   execution engine of DuckDB. It effectively represents a subset of a relation.
   It holds a set of vectors that all have the same length.

    DataChunk is initialized using the DataChunk::Initialize function by
   providing it with a vector of TypeIds for the Vector members. By default,
   this function will also allocate a chunk of memory in the DataChunk for the
   vectors and all the vectors will be referencing vectors to the data owned by
   the chunk. The reason for this behavior is that the underlying vectors can
   become referencing vectors to other chunks as well (i.e. in the case an
   operator does not alter the data, such as a Filter operator which only adds a
   selection vector).

    In addition to holding the data of the vectors, the DataChunk also owns the
   selection vector that underlying vectors can point to.
*/
class DataChunk {
public:
	//! Creates an empty DataChunk
	DataChunk();

	//! The vectors owned by the DataChunk.
	vector<Vector> data;

public:
	idx_t size() const {
		return count;
	}
	idx_t column_count() const {
		return data.size();
	}
	void SetCardinality(idx_t count) {
		assert(count <= STANDARD_VECTOR_SIZE);
		this->count = count;
	}
	void SetCardinality(const DataChunk &other) {
		this->count = other.size();
	}

	Value GetValue(idx_t col_idx, idx_t index) const;
	void SetValue(idx_t col_idx, idx_t index, Value val);

	//! Set the DataChunk to reference another data chunk
	void Reference(DataChunk &chunk);

	//! Initializes the DataChunk with the specified types to an empty DataChunk
	//! This will create one vector of the specified type for each LogicalType in the
	//! types list. The vector will be referencing vector to the data owned by
	//! the DataChunk.
	void Initialize(vector<LogicalType> &types);
	//! Initializes an empty DataChunk with the given types. The vectors will *not* have any data allocated for them.
	void InitializeEmpty(vector<LogicalType> &types);
	//! Append the other DataChunk to this one. The column count and types of
	//! the two DataChunks have to match exactly. Throws an exception if there
	//! is not enough space in the chunk.
	void Append(DataChunk &other);
	//! Destroy all data and columns owned by this DataChunk
	void Destroy();

	//! Copies the data from this vector to another vector.
	void Copy(DataChunk &other, idx_t offset = 0);

	//! Turn all the vectors from the chunk into flat vectors
	void Normalify();

	unique_ptr<VectorData[]> Orrify();

	void Slice(const SelectionVector &sel_vector, idx_t count);
	void Slice(DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset = 0);

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	void Reset();

	//! Serializes a DataChunk to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a DataChunk
	void Deserialize(Deserializer &source);

	//! Hashes the DataChunk to the target vector
	void Hash(Vector &result);

	//! Returns a list of types of the vectors of this data chunk
	vector<LogicalType> GetTypes();

	//! Converts this DataChunk to a printable string representation
	string ToString() const;
	void Print();

	DataChunk(const DataChunk &) = delete;

	//! Verify that the DataChunk is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	void Verify();

	//! export data chunk as a arrow struct array that can be imported as arrow record batch
	void ToArrowArray(ArrowArray *out_array);

private:
	idx_t count;
};
} // namespace duckdb


namespace duckdb {

//!  A ChunkCollection represents a set of DataChunks that all have the same
//!  types
/*!
    A ChunkCollection represents a set of DataChunks concatenated together in a
   list. Individual values of the collection can be iterated over using the
   iterator. It is also possible to iterate directly over the chunks for more
   direct access.
*/
class ChunkCollection {
public:
	ChunkCollection() : count(0) {
	}

	//! The total amount of elements in the collection
	idx_t count;
	//! The set of data chunks in the collection
	vector<unique_ptr<DataChunk>> chunks;
	//! The types of the ChunkCollection
	vector<LogicalType> types;

	//! The amount of columns in the ChunkCollection
	idx_t column_count() {
		return types.size();
	}

	//! Append a new DataChunk directly to this ChunkCollection
	void Append(DataChunk &new_chunk);

	//! Append another ChunkCollection directly to this ChunkCollection
	void Append(ChunkCollection &other);

	void Verify();

	//! Gets the value of the column at the specified index
	Value GetValue(idx_t column, idx_t index);
	//! Sets the value of the column at the specified index
	void SetValue(idx_t column, idx_t index, Value value);

	vector<Value> GetRow(idx_t index);

	string ToString() const {
		return chunks.size() == 0 ? "ChunkCollection [ 0 ]"
		                          : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
	}
	void Print();

	//! Gets a reference to the chunk at the given index
	DataChunk &GetChunk(idx_t index) {
		return *chunks[LocateChunk(index)];
	}

	void Sort(vector<OrderType> &desc, vector<OrderByNullType> &null_order, idx_t result[]);
	//! Reorders the rows in the collection according to the given indices. NB: order is changed!
	void Reorder(idx_t order[]);

	void MaterializeSortedChunk(DataChunk &target, idx_t order[], idx_t start_offset);

	//! Returns true if the ChunkCollections are equivalent
	bool Equals(ChunkCollection &other);

	//! Locates the chunk that belongs to the specific index
	idx_t LocateChunk(idx_t index) {
		idx_t result = index / STANDARD_VECTOR_SIZE;
		assert(result < chunks.size());
		return result;
	}

	void Heap(vector<OrderType> &desc, vector<OrderByNullType> &null_order, idx_t heap[], idx_t heap_size);
	idx_t MaterializeHeapChunk(DataChunk &target, idx_t order[], idx_t start_offset, idx_t heap_size);
};
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/statement_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType : uint8_t {
	INVALID_STATEMENT,      // invalid statement type
	SELECT_STATEMENT,       // select statement type
	INSERT_STATEMENT,       // insert statement type
	UPDATE_STATEMENT,       // update statement type
	CREATE_STATEMENT,       // create statement type
	DELETE_STATEMENT,       // delete statement type
	PREPARE_STATEMENT,      // prepare statement type
	EXECUTE_STATEMENT,      // execute statement type
	ALTER_STATEMENT,        // alter statement type
	TRANSACTION_STATEMENT,  // transaction statement type,
	COPY_STATEMENT,         // copy type
	ANALYZE_STATEMENT,      // analyze type
	VARIABLE_SET_STATEMENT, // variable set statement type
	CREATE_FUNC_STATEMENT,  // create func statement type
	EXPLAIN_STATEMENT,      // explain statement type
	DROP_STATEMENT,         // DROP statement type
	EXPORT_STATEMENT,       // EXPORT statement type
	PRAGMA_STATEMENT,       // PRAGMA statement type
	VACUUM_STATEMENT,       // VACUUM statement type
	RELATION_STATEMENT
};

string StatementTypeToString(StatementType type);

} // namespace duckdb


struct ArrowSchema;

namespace duckdb {

enum class QueryResultType : uint8_t { MATERIALIZED_RESULT, STREAM_RESULT };

//! The QueryResult object holds the result of a query. It can either be a MaterializedQueryResult, in which case the
//! result contains the entire result set, or a StreamQueryResult in which case the Fetch method can be called to
//! incrementally fetch data from the database.
class QueryResult {
public:
	//! Creates an successful empty query result
	QueryResult(QueryResultType type, StatementType statement_type);
	//! Creates a successful query result with the specified names and types
	QueryResult(QueryResultType type, StatementType statement_type, vector<LogicalType> types, vector<string> names);
	//! Creates an unsuccessful query result with error condition
	QueryResult(QueryResultType type, string error);
	virtual ~QueryResult() {
	}

	//! The type of the result (MATERIALIZED or STREAMING)
	QueryResultType type;
	//! The type of the statement that created this result
	StatementType statement_type;
	//! The SQL types of the result
	vector<LogicalType> types;
	//! The names of the result
	vector<string> names;
	//! Whether or not execution was successful
	bool success;
	//! The error string (in case execution was not successful)
	string error;
	//! The next result (if any)
	unique_ptr<QueryResult> next;

public:
	//! Fetches a DataChunk from the query result. Returns an empty chunk if the result is empty, or nullptr on failure.
	virtual unique_ptr<DataChunk> Fetch() = 0;
	// Converts the QueryResult to a string
	virtual string ToString() = 0;
	//! Prints the QueryResult to the console
	void Print();
	//! Returns true if the two results are identical; false otherwise. Note that this method is destructive; it calls
	//! Fetch() until both results are exhausted. The data in the results will be lost.
	bool Equals(QueryResult &other);

	idx_t column_count() {
		return types.size();
	}

	void ToArrowSchema(ArrowSchema *out_array);

private:
	//! The current chunk used by the iterator
	unique_ptr<DataChunk> iterator_chunk;

	class QueryResultIterator;

	class QueryResultRow {
	public:
		QueryResultRow(QueryResultIterator &iterator) : iterator(iterator), row(0) {
		}

		QueryResultIterator &iterator;
		idx_t row;

		template <class T> T GetValue(idx_t col_idx) const {
			return iterator.result->iterator_chunk->GetValue(col_idx, iterator.row_idx).GetValue<T>();
		}
	};
	//! The row-based query result iterator. Invoking the
	class QueryResultIterator {
	public:
		QueryResultIterator(QueryResult *result) : current_row(*this), result(result), row_idx(0) {
			if (result) {
				result->iterator_chunk = result->Fetch();
			}
		}

		QueryResultRow current_row;
		QueryResult *result;
		idx_t row_idx;

	public:
		void Next() {
			if (!result->iterator_chunk) {
				return;
			}
			current_row.row++;
			row_idx++;
			if (row_idx >= result->iterator_chunk->size()) {
				result->iterator_chunk = result->Fetch();
				row_idx = 0;
			}
		}

		QueryResultIterator &operator++() {
			Next();
			return *this;
		}
		bool operator!=(const QueryResultIterator &other) const {
			return result->iterator_chunk && result->iterator_chunk->column_count() > 0;
		}
		const QueryResultRow &operator*() const {
			return current_row;
		}
	};

public:
	QueryResultIterator begin() {
		return QueryResultIterator(this);
	}
	QueryResultIterator end() {
		return QueryResultIterator(nullptr);
	}

protected:
	string HeaderToString();

private:
	QueryResult(const QueryResult &) = delete;
};

} // namespace duckdb


namespace duckdb {

class MaterializedQueryResult : public QueryResult {
public:
	//! Creates an empty successful query result
	MaterializedQueryResult(StatementType statement_type);
	//! Creates a successful query result with the specified names and types
	MaterializedQueryResult(StatementType statement_type, vector<LogicalType> types, vector<string> names);
	//! Creates an unsuccessful query result with error condition
	MaterializedQueryResult(string error);

	//! Fetches a DataChunk from the query result. Returns an empty chunk if the result is empty, or nullptr on failure.
	//! This will consume the result (i.e. the chunks are taken directly from the ChunkCollection).
	unique_ptr<DataChunk> Fetch() override;
	//! Converts the QueryResult to a string
	string ToString() override;

	//! Gets the (index) value of the (column index) column
	Value GetValue(idx_t column, idx_t index);

	template <class T> T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}

	ChunkCollection collection;
};

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/stream_query_result.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

class ClientContext;
class MaterializedQueryResult;

class StreamQueryResult : public QueryResult {
public:
	//! Create a successful StreamQueryResult. StreamQueryResults should always be successful initially (it makes no
	//! sense to stream an error).
	StreamQueryResult(StatementType statement_type, ClientContext &context, vector<LogicalType> types,
	                  vector<string> names);
	~StreamQueryResult() override;

	//! Fetches a DataChunk from the query result. Returns an empty chunk if the result is empty, or nullptr on error.
	unique_ptr<DataChunk> Fetch() override;
	//! Converts the QueryResult to a string
	string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	unique_ptr<MaterializedQueryResult> Materialize();

	//! Closes the StreamQueryResult
	void Close();

	//! Whether or not the StreamQueryResult is still open
	bool is_open;

private:
	//! The client context this StreamQueryResult belongs to
	ClientContext &context;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! A prepared statement
class PreparedStatement {
public:
	//! Create a successfully prepared prepared statement object with the given name
	PreparedStatement(ClientContext *context, string name, string query, PreparedStatementData &data,
	                  idx_t n_param = 0);
	//! Create a prepared statement that was not successfully prepared
	PreparedStatement(string error);

	~PreparedStatement();

public:
	StatementType type;
	//! The client context this prepared statement belongs to
	ClientContext *context;
	//! The internal name of the prepared statement
	string name;
	//! The query that is being prepared
	string query;
	//! Whether or not the statement was successfully prepared
	bool success;
	//! The error message (if success = false)
	string error;
	//! Whether or not the prepared statement has been invalidated because the underlying connection has been destroyed
	bool is_invalidated;
	//! The amount of bound parameters
	idx_t n_param;
	//! The result SQL types of the prepared statement
	vector<LogicalType> types;
	//! The result names of the prepared statement
	vector<string> names;

public:
	//! Execute the prepared statement with the given set of arguments
	template <typename... Args> unique_ptr<QueryResult> Execute(Args... args) {
		vector<Value> values;
		return ExecuteRecursive(values, args...);
	}

	//! Execute the prepared statement with the given set of values
	unique_ptr<QueryResult> Execute(vector<Value> &values, bool allow_stream_result = true);

private:
	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values) {
		return Execute(values);
	}

	template <typename T, typename... Args>
	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return ExecuteRecursive(values, args...);
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/table_description.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/column_definition.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_expression.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/base_expression.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/expression_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//
enum class ExpressionType : uint8_t {
	INVALID = 0,

	// explicitly cast left as right (right is integer in ValueType enum)
	OPERATOR_CAST = 12,
	// logical not operator
	OPERATOR_NOT = 13,
	// is null operator
	OPERATOR_IS_NULL = 14,
	// is not null operator
	OPERATOR_IS_NOT_NULL = 15,

	// -----------------------------
	// Comparison Operators
	// -----------------------------
	// equal operator between left and right
	COMPARE_EQUAL = 25,
	// compare initial boundary
	COMPARE_BOUNDARY_START = COMPARE_EQUAL,
	// inequal operator between left and right
	COMPARE_NOTEQUAL = 26,
	// less than operator between left and right
	COMPARE_LESSTHAN = 27,
	// greater than operator between left and right
	COMPARE_GREATERTHAN = 28,
	// less than equal operator between left and right
	COMPARE_LESSTHANOREQUALTO = 29,
	// greater than equal operator between left and right
	COMPARE_GREATERTHANOREQUALTO = 30,
	// IN operator [left IN (right1, right2, ...)]
	COMPARE_IN = 35,
	// NOT IN operator [left NOT IN (right1, right2, ...)]
	COMPARE_NOT_IN = 36,
	// IS DISTINCT FROM operator
	COMPARE_DISTINCT_FROM = 37,
	// compare final boundary

	COMPARE_BETWEEN = 38,
	COMPARE_NOT_BETWEEN = 39,
	COMPARE_BOUNDARY_END = COMPARE_NOT_BETWEEN,

	// -----------------------------
	// Conjunction Operators
	// -----------------------------
	CONJUNCTION_AND = 50,
	CONJUNCTION_OR = 51,

	// -----------------------------
	// Values
	// -----------------------------
	VALUE_CONSTANT = 75,
	VALUE_PARAMETER = 76,
	VALUE_TUPLE = 77,
	VALUE_TUPLE_ADDRESS = 78,
	VALUE_NULL = 79,
	VALUE_VECTOR = 80,
	VALUE_SCALAR = 81,
	VALUE_DEFAULT = 82,

	// -----------------------------
	// Aggregates
	// -----------------------------
	AGGREGATE = 100,
	BOUND_AGGREGATE = 101,

	// -----------------------------
	// Window Functions
	// -----------------------------
	WINDOW_AGGREGATE = 110,

	WINDOW_RANK = 120,
	WINDOW_RANK_DENSE = 121,
	WINDOW_NTILE = 122,
	WINDOW_PERCENT_RANK = 123,
	WINDOW_CUME_DIST = 124,
	WINDOW_ROW_NUMBER = 125,

	WINDOW_FIRST_VALUE = 130,
	WINDOW_LAST_VALUE = 131,
	WINDOW_LEAD = 132,
	WINDOW_LAG = 133,

	// -----------------------------
	// Functions
	// -----------------------------
	FUNCTION = 140,
	BOUND_FUNCTION = 141,

	// -----------------------------
	// Operators
	// -----------------------------
	CASE_EXPR = 150,
	OPERATOR_NULLIF = 151,
	OPERATOR_COALESCE = 152,

	// -----------------------------
	// Subquery IN/EXISTS
	// -----------------------------
	SUBQUERY = 175,

	// -----------------------------
	// Parser
	// -----------------------------
	STAR = 200,
	TABLE_STAR = 201,
	PLACEHOLDER = 202,
	COLUMN_REF = 203,
	FUNCTION_REF = 204,
	TABLE_REF = 205,

	// -----------------------------
	// Miscellaneous
	// -----------------------------
	CAST = 225,
	COMMON_SUBEXPRESSION = 226,
	BOUND_REF = 227,
	BOUND_COLUMN_REF = 228,
	BOUND_UNNEST = 229,
	COLLATE = 230
};

//===--------------------------------------------------------------------===//
// Expression Class
//===--------------------------------------------------------------------===//
enum class ExpressionClass : uint8_t {
	INVALID = 0,
	//===--------------------------------------------------------------------===//
	// Parsed Expressions
	//===--------------------------------------------------------------------===//
	AGGREGATE = 1,
	CASE = 2,
	CAST = 3,
	COLUMN_REF = 4,
	COMPARISON = 5,
	CONJUNCTION = 6,
	CONSTANT = 7,
	DEFAULT = 8,
	FUNCTION = 9,
	OPERATOR = 10,
	STAR = 11,
	TABLE_STAR = 12,
	SUBQUERY = 13,
	WINDOW = 14,
	PARAMETER = 15,
	COLLATE = 16,
	//===--------------------------------------------------------------------===//
	// Bound Expressions
	//===--------------------------------------------------------------------===//
	BOUND_AGGREGATE = 25,
	BOUND_CASE = 26,
	BOUND_CAST = 27,
	BOUND_COLUMN_REF = 28,
	BOUND_COMPARISON = 29,
	BOUND_CONJUNCTION = 30,
	BOUND_CONSTANT = 31,
	BOUND_DEFAULT = 32,
	BOUND_FUNCTION = 33,
	BOUND_OPERATOR = 34,
	BOUND_PARAMETER = 35,
	BOUND_REF = 36,
	BOUND_SUBQUERY = 37,
	BOUND_WINDOW = 38,
	BOUND_BETWEEN = 39,
	BOUND_UNNEST = 40,
	//===--------------------------------------------------------------------===//
	// Miscellaneous
	//===--------------------------------------------------------------------===//
	BOUND_EXPRESSION = 50,
	COMMON_SUBEXPRESSION = 51
};

string ExpressionTypeToString(ExpressionType type);
string ExpressionTypeToOperator(ExpressionType type);

//! Negate a comparison expression, turning e.g. = into !=, or < into >=
ExpressionType NegateComparisionExpression(ExpressionType type);
//! Flip a comparison expression, turning e.g. < into >, or = into =
ExpressionType FlipComparisionExpression(ExpressionType type);

} // namespace duckdb


namespace duckdb {

//!  The BaseExpression class is a base class that can represent any expression
//!  part of a SQL statement.
class BaseExpression {
public:
	//! Create an Expression
	BaseExpression(ExpressionType type, ExpressionClass expression_class)
	    : type(type), expression_class(expression_class) {
	}
	virtual ~BaseExpression() {
	}

	//! Returns the type of the expression
	ExpressionType GetExpressionType() {
		return type;
	}
	//! Returns the class of the expression
	ExpressionClass GetExpressionClass() {
		return expression_class;
	}

	//! Type of the expression
	ExpressionType type;
	//! The expression class of the node
	ExpressionClass expression_class;
	//! The alias of the expression,
	string alias;

public:
	//! Returns true if this expression is an aggregate or not.
	/*!
	 Examples:

	 (1) SUM(a) + 1 -- True

	 (2) a + 1 -- False
	 */
	virtual bool IsAggregate() const = 0;
	//! Returns true if the expression has a window function or not
	virtual bool IsWindow() const = 0;
	//! Returns true if the query contains a subquery
	virtual bool HasSubquery() const = 0;
	//! Returns true if expression does not contain a group ref or col ref or parameter
	virtual bool IsScalar() const = 0;
	//! Returns true if the expression has a parameter
	virtual bool HasParameter() const = 0;

	//! Get the name of the expression
	virtual string GetName() const {
		return !alias.empty() ? alias : ToString();
	}
	//! Convert the Expression to a String
	virtual string ToString() const = 0;
	//! Print the expression to stdout
	void Print();

	//! Creates a hash value of this expression. It is important that if two expressions are identical (i.e.
	//! Expression::Equals() returns true), that their hash value is identical as well.
	virtual hash_t Hash() const = 0;
	//! Returns true if this expression is equal to another expression
	virtual bool Equals(const BaseExpression *other) const;

	static bool Equals(BaseExpression *left, BaseExpression *right) {
		if (left == right) {
			return true;
		}
		if (!left || !right) {
			return false;
		}
		return left->Equals(right);
	}
	bool operator==(const BaseExpression &rhs) {
		return this->Equals(&rhs);
	}
};

} // namespace duckdb


namespace duckdb {
class Serializer;
class Deserializer;

//!  The ParsedExpression class is a base class that can represent any expression
//!  part of a SQL statement.
/*!
 The ParsedExpression class is a base class that can represent any expression
 part of a SQL statement. This is, for example, a column reference in a SELECT
 clause, but also operators, aggregates or filters. The Expression is emitted by the parser and does not contain any
 information about bindings to the catalog or to the types. ParsedExpressions are transformed into regular Expressions
 in the Binder.
 */
class ParsedExpression : public BaseExpression {
public:
	//! Create an Expression
	ParsedExpression(ExpressionType type, ExpressionClass expression_class) : BaseExpression(type, expression_class) {
	}

public:
	bool IsAggregate() const override;
	bool IsWindow() const override;
	bool HasSubquery() const override;
	bool IsScalar() const override;
	bool HasParameter() const override;

	bool Equals(const BaseExpression *other) const override;
	hash_t Hash() const override;

	//! Create a copy of this expression
	virtual unique_ptr<ParsedExpression> Copy() const = 0;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into an Expression [CAN THROW:
	//! SerializationException]
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &source);

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(const ParsedExpression &other) {
		type = other.type;
		expression_class = other.expression_class;
		alias = other.alias;
	}
};

} // namespace duckdb


namespace duckdb {

//! A column of a table.
class ColumnDefinition {
public:
	ColumnDefinition(string name, LogicalType type) : name(name), type(type) {
	}
	ColumnDefinition(string name, LogicalType type, unique_ptr<ParsedExpression> default_value)
	    : name(name), type(type), default_value(move(default_value)) {
	}

	//! The name of the entry
	string name;
	//! The index of the column in the table
	idx_t oid;
	//! The type of the column
	LogicalType type;
	//! The default value of the column (if any)
	unique_ptr<ParsedExpression> default_value;

public:
	ColumnDefinition Copy();

	void Serialize(Serializer &serializer);
	static ColumnDefinition Deserialize(Deserializer &source);
};

} // namespace duckdb


namespace duckdb {

struct TableDescription {
	//! The schema of the table
	string schema;
	//! The table name of the table
	string table;
	//! The columns of the table
	vector<ColumnDefinition> columns;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/relation_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class RelationType : uint8_t {
	INVALID_RELATION,
	TABLE_RELATION,
	PROJECTION_RELATION,
	FILTER_RELATION,
	EXPLAIN_RELATION,
	CROSS_PRODUCT_RELATION,
	JOIN_RELATION,
	AGGREGATE_RELATION,
	SET_OPERATION_RELATION,
	DISTINCT_RELATION,
	LIMIT_RELATION,
	ORDER_RELATION,
	CREATE_VIEW_RELATION,
	CREATE_TABLE_RELATION,
	INSERT_RELATION,
	VALUE_LIST_RELATION,
	DELETE_RELATION,
	UPDATE_RELATION,
	WRITE_CSV_RELATION,
	READ_CSV_RELATION,
	SUBQUERY_RELATION,
	TABLE_FUNCTION_RELATION,
	VIEW_RELATION
};

string RelationTypeToString(RelationType type);

} // namespace duckdb



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/join_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//
enum class JoinType : uint8_t {
	INVALID = 0, // invalid join type
	LEFT = 1,    // left
	RIGHT = 2,   // right
	INNER = 3,   // inner
	OUTER = 4,   // outer
	SEMI = 5,    // SEMI join returns left side row ONLY if it has a join partner, no duplicates
	ANTI = 6,    // ANTI join returns left side row ONLY if it has NO join partner, no duplicates
	MARK = 7,    // MARK join returns marker indicating whether or not there is a join partner (true), there is no join
	             // partner (false)
	SINGLE = 8   // SINGLE join is like LEFT OUTER JOIN, BUT returns at most one join partner per entry on the LEFT side
	             // (and NULL if no partner is found)
};

string JoinTypeToString(JoinType type);

} // namespace duckdb


#include <memory>

namespace duckdb {
struct BoundStatement;

class ClientContext;
class Binder;
class LogicalOperator;
class QueryNode;
class TableRef;

class Relation : public std::enable_shared_from_this<Relation> {
public:
	Relation(ClientContext &context, RelationType type) : context(context), type(type) {
	}
	virtual ~Relation() {
	}

	ClientContext &context;
	RelationType type;

public:
	virtual const vector<ColumnDefinition> &Columns() = 0;
	virtual unique_ptr<QueryNode> GetQueryNode() = 0;
	virtual BoundStatement Bind(Binder &binder);
	virtual string GetAlias();

	unique_ptr<QueryResult> Execute();
	string ToString();
	virtual string ToString(idx_t depth) = 0;

	void Print();
	void Head(idx_t limit = 10);

	shared_ptr<Relation> CreateView(string name, bool replace = true);
	unique_ptr<QueryResult> Query(string sql);
	unique_ptr<QueryResult> Query(string name, string sql);

	//! Explain the query plan of this relation
	unique_ptr<QueryResult> Explain();

	virtual unique_ptr<TableRef> GetTableRef();
	virtual bool IsReadOnly() {
		return true;
	}

public:
	// PROJECT
	shared_ptr<Relation> Project(string select_list);
	shared_ptr<Relation> Project(string expression, string alias);
	shared_ptr<Relation> Project(string select_list, vector<string> aliases);
	shared_ptr<Relation> Project(vector<string> expressions);
	shared_ptr<Relation> Project(vector<string> expressions, vector<string> aliases);

	// FILTER
	shared_ptr<Relation> Filter(string expression);
	shared_ptr<Relation> Filter(vector<string> expressions);

	// LIMIT
	shared_ptr<Relation> Limit(int64_t n, int64_t offset = 0);

	// ORDER
	shared_ptr<Relation> Order(string expression);
	shared_ptr<Relation> Order(vector<string> expressions);

	// JOIN operation
	shared_ptr<Relation> Join(shared_ptr<Relation> other, string condition, JoinType type = JoinType::INNER);

	// SET operations
	shared_ptr<Relation> Union(shared_ptr<Relation> other);
	shared_ptr<Relation> Except(shared_ptr<Relation> other);
	shared_ptr<Relation> Intersect(shared_ptr<Relation> other);

	// DISTINCT operation
	shared_ptr<Relation> Distinct();

	// AGGREGATES
	shared_ptr<Relation> Aggregate(string aggregate_list);
	shared_ptr<Relation> Aggregate(vector<string> aggregates);
	shared_ptr<Relation> Aggregate(string aggregate_list, string group_list);
	shared_ptr<Relation> Aggregate(vector<string> aggregates, vector<string> groups);

	// ALIAS
	shared_ptr<Relation> Alias(string alias);

	//! Insert the data from this relation into a table
	void Insert(string table_name);
	void Insert(string schema_name, string table_name);
	//! Insert a row (i.e.,list of values) into a table
    void Insert(vector<vector<Value>> values);
	//! Create a table and insert the data from this relation into that table
	void Create(string table_name);
	void Create(string schema_name, string table_name);

	//! Write a relation to a CSV file
	void WriteCSV(string csv_file);

	//! Update a table, can only be used on a TableRelation
	virtual void Update(string update, string condition = string());
	//! Delete from a table, can only be used on a TableRelation
	virtual void Delete(string condition = string());

public:
	//! Whether or not the relation inherits column bindings from its child or not, only relevant for binding
	virtual bool InheritsColumnBindings() {
		return false;
	}
	virtual Relation *ChildRelation() {
		return nullptr;
	}

protected:
	string RenderWhitespace(idx_t depth);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/profiler_format.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

enum class ProfilerPrintFormat : uint8_t { NONE, QUERY_TREE, JSON };

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/sql_statement.hpp
//
//
//===----------------------------------------------------------------------===//






//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/printer.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! Printer is a static class that allows printing to logs or stdout/stderr
class Printer {
public:
	//! Print the object to stderr
	static void Print(string str);
};
} // namespace duckdb


namespace duckdb {
//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement {
public:
	SQLStatement(StatementType type) : type(type){};
	virtual ~SQLStatement() {
	}

	StatementType type;
	idx_t stmt_location;
	idx_t stmt_length;
};
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/udf_function.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/binary_executor.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/vector_operations.hpp
//
//
//===----------------------------------------------------------------------===//






#include <functional>

namespace duckdb {

// VectorOperations contains a set of operations that operate on sets of
// vectors. In general, the operators must all have the same type, otherwise an
// exception is thrown. Note that the functions underneath use restrict
// pointers, hence the data that the vectors point to (and hence the vector
// themselves) should not be equal! For example, if you call the function Add(A,
// B, A) then ASSERT_RESTRICT will be triggered. Instead call AddInPlace(A, B)
// or Add(A, B, C)
struct VectorOperations {
	//===--------------------------------------------------------------------===//
	// In-Place Operators
	//===--------------------------------------------------------------------===//
	//! A += B
	static void AddInPlace(Vector &A, int64_t B, idx_t count);

	//===--------------------------------------------------------------------===//
	// NULL Operators
	//===--------------------------------------------------------------------===//
	//! result = IS NOT NULL(A)
	static void IsNotNull(Vector &A, Vector &result, idx_t count);
	//! result = IS NULL (A)
	static void IsNull(Vector &A, Vector &result, idx_t count);
	// Returns whether or not a vector has a NULL value
	static bool HasNull(Vector &A, idx_t count);
	static bool HasNotNull(Vector &A, idx_t count);

	//===--------------------------------------------------------------------===//
	// Boolean Operations
	//===--------------------------------------------------------------------===//
	// result = A && B
	static void And(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A || B
	static void Or(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = NOT(A)
	static void Not(Vector &A, Vector &result, idx_t count);

	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// result = A == B
	static void Equals(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A != B
	static void NotEquals(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A > B
	static void GreaterThan(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A >= B
	static void GreaterThanEquals(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A < B
	static void LessThan(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A <= B
	static void LessThanEquals(Vector &A, Vector &B, Vector &result, idx_t count);

	//===--------------------------------------------------------------------===//
	// Scatter methods
	//===--------------------------------------------------------------------===//
	// make sure dest.count is set for gather methods!
	struct Gather {
		//! dest.data[i] = ptr[i]. NullValue<T> is checked for and converted to the nullmask in dest. The source
		//! addresses are incremented by the size of the type.
		static void Set(Vector &source, Vector &dest, idx_t count);
	};

	//===--------------------------------------------------------------------===//
	// Hash functions
	//===--------------------------------------------------------------------===//
	// result = HASH(A)
	static void Hash(Vector &input, Vector &hashes, idx_t count);
	static void Hash(Vector &input, Vector &hashes, const SelectionVector &rsel, idx_t count);
	// A ^= HASH(B)
	static void CombineHash(Vector &hashes, Vector &B, idx_t count);
	static void CombineHash(Vector &hashes, Vector &B, const SelectionVector &rsel, idx_t count);

	//===--------------------------------------------------------------------===//
	// Generate functions
	//===--------------------------------------------------------------------===//
	static void GenerateSequence(Vector &result, idx_t count, int64_t start = 0, int64_t increment = 1);
	static void GenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start = 0,
	                             int64_t increment = 1);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	// Cast the data from the source type to the target type
	static void Cast(Vector &source, Vector &result, idx_t count, bool strict = false);

	// Copy the data of <source> to the target vector
	static void Copy(Vector &source, Vector &target, idx_t source_count, idx_t source_offset, idx_t target_offset);
	static void Copy(Vector &source, Vector &target, const SelectionVector &sel, idx_t source_count,
	                 idx_t source_offset, idx_t target_offset);

	// Copy the data of <source> to the target location, setting null values to
	// NullValue<T>. Used to store data without separate NULL mask.
	static void WriteToStorage(Vector &source, idx_t count, data_ptr_t target);
	// Reads the data of <source> to the target vector, setting the nullmask
	// for any NullValue<T> of source. Used to go back from storage to a proper vector
	static void ReadFromStorage(data_ptr_t source, idx_t count, Vector &result);
};
} // namespace duckdb

#include <functional>

namespace duckdb {

struct DefaultNullCheckOperator {
	template <class LEFT_TYPE, class RIGHT_TYPE> static inline bool Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return false;
	}
};

struct BinaryStandardOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, nullmask_t &nullmask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
	}
};

struct BinarySingleArgumentOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, nullmask_t &nullmask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE>(left, right);
	}
};

struct BinaryLambdaWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, nullmask_t &nullmask, idx_t idx) {
		return fun(left, right);
	}
};

struct BinaryExecutor {
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool IGNORE_NULL, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static void ExecuteFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                            RESULT_TYPE *__restrict result_data, idx_t count, nullmask_t &nullmask, FUNC fun) {
		if (!LEFT_CONSTANT) {
			ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
		}
		if (!RIGHT_CONSTANT) {
			ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
		}
		if (IGNORE_NULL && nullmask.any()) {
			for (idx_t i = 0; i < count; i++) {
				if (!nullmask[i]) {
					auto lentry = ldata[LEFT_CONSTANT ? 0 : i];
					auto rentry = rdata[RIGHT_CONSTANT ? 0 : i];
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
					    fun, lentry, rentry, nullmask, i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lentry = ldata[LEFT_CONSTANT ? 0 : i];
				auto rentry = rdata[RIGHT_CONSTANT ? 0 : i];
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
				    fun, lentry, rentry, nullmask, i);
			}
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool IGNORE_NULL>
	static void ExecuteConstant(Vector &left, Vector &right, Vector &result, FUNC fun) {
		result.vector_type = VectorType::CONSTANT_VECTOR;

		auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
		auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);
		auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);

		if (ConstantVector::IsNull(left) || ConstantVector::IsNull(right)) {
			ConstantVector::SetNull(result, true);
			return;
		}
		*result_data = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
		    fun, *ldata, *rdata, ConstantVector::Nullmask(result), 0);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool IGNORE_NULL, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static void ExecuteFlat(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
		auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);

		if ((LEFT_CONSTANT && ConstantVector::IsNull(left)) || (RIGHT_CONSTANT && ConstantVector::IsNull(right))) {
			// either left or right is constant NULL: result is constant NULL
			result.vector_type = VectorType::CONSTANT_VECTOR;
			ConstantVector::SetNull(result, true);
			return;
		}

		result.vector_type = VectorType::FLAT_VECTOR;
		auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
		if (LEFT_CONSTANT) {
			FlatVector::SetNullmask(result, FlatVector::Nullmask(right));
		} else if (RIGHT_CONSTANT) {
			FlatVector::SetNullmask(result, FlatVector::Nullmask(left));
		} else {
			FlatVector::SetNullmask(result, FlatVector::Nullmask(left) | FlatVector::Nullmask(right));
		}
		ExecuteFlatLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, LEFT_CONSTANT,
		                RIGHT_CONSTANT>(ldata, rdata, result_data, count, FlatVector::Nullmask(result), fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool IGNORE_NULL>
	static void ExecuteGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                               RESULT_TYPE *__restrict result_data, const SelectionVector *__restrict lsel,
	                               const SelectionVector *__restrict rsel, idx_t count, nullmask_t &lnullmask,
	                               nullmask_t &rnullmask, nullmask_t &result_nullmask, FUNC fun) {
		if (lnullmask.any() || rnullmask.any()) {
			for (idx_t i = 0; i < count; i++) {
				auto lindex = lsel->get_index(i);
				auto rindex = rsel->get_index(i);
				if (!lnullmask[lindex] && !rnullmask[rindex]) {
					auto lentry = ldata[lindex];
					auto rentry = rdata[rindex];
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
					    fun, lentry, rentry, result_nullmask, i);
				} else {
					result_nullmask[i] = true;
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lentry = ldata[lsel->get_index(i)];
				auto rentry = rdata[rsel->get_index(i)];
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
				    fun, lentry, rentry, result_nullmask, i);
			}
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool IGNORE_NULL>
	static void ExecuteGeneric(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		VectorData ldata, rdata;

		left.Orrify(count, ldata);
		right.Orrify(count, rdata);

		result.vector_type = VectorType::FLAT_VECTOR;
		auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
		ExecuteGenericLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
		    (LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data, result_data, ldata.sel, rdata.sel, count,
		    *ldata.nullmask, *rdata.nullmask, FlatVector::Nullmask(result), fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool IGNORE_NULL>
	static void ExecuteSwitch(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(left, right, result,
			                                                                                      fun);
		} else if (left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, false, true>(
			    left, right, result, count, fun);
		} else if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, true, false>(
			    left, right, result, count, fun);
		} else if (left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, false, false>(
			    left, right, result, count, fun);
		} else {
			ExecuteGeneric<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(left, right, result,
			                                                                                     count, fun);
		}
	}

public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, bool IGNORE_NULL = false,
	          class FUNC = std::function<RESULT_TYPE(LEFT_TYPE, RIGHT_TYPE)>>
	static void Execute(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryLambdaWrapper, bool, FUNC, IGNORE_NULL>(
		    left, right, result, count, fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false,
	          class OPWRAPPER = BinarySingleArgumentOperatorWrapper>
	static void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool, IGNORE_NULL>(left, right, result, count,
		                                                                                    false);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false>
	static void ExecuteStandard(Vector &left, Vector &right, Vector &result, idx_t count) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryStandardOperatorWrapper, OP, bool, IGNORE_NULL>(
		    left, right, result, count, false);
	}

public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t SelectConstant(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                            SelectionVector *true_sel, SelectionVector *false_sel) {
		auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
		auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);

		// both sides are constant, return either 0 or the count
		// in this case we do not fill in the result selection vector at all
		if (ConstantVector::IsNull(left) || ConstantVector::IsNull(right) || !OP::Operation(*ldata, *rdata)) {
			if (false_sel) {
				for (idx_t i = 0; i < count; i++) {
					false_sel->set_index(i, sel->get_index(i));
				}
			}
			return 0;
		} else {
			if (true_sel) {
				for (idx_t i = 0; i < count; i++) {
					true_sel->set_index(i, sel->get_index(i));
				}
			}
			return count;
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL,
	          bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static inline idx_t SelectFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                                   const SelectionVector *sel, idx_t count, nullmask_t &nullmask,
	                                   SelectionVector *true_sel, SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			idx_t result_idx = sel->get_index(i);
			idx_t lidx = LEFT_CONSTANT ? 0 : i;
			idx_t ridx = RIGHT_CONSTANT ? 0 : i;
			if ((NO_NULL || !nullmask[i]) && OP::Operation(ldata[lidx], rdata[ridx])) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL>
	static inline idx_t SelectFlatLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                                            const SelectionVector *sel, idx_t count, nullmask_t &nullmask,
	                                            SelectionVector *true_sel, SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, true>(
			    ldata, rdata, sel, count, nullmask, true_sel, false_sel);
		} else if (true_sel) {
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, false>(
			    ldata, rdata, sel, count, nullmask, true_sel, false_sel);
		} else {
			assert(false_sel);
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, false, true>(
			    ldata, rdata, sel, count, nullmask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static inline idx_t SelectFlatLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                                         const SelectionVector *sel, idx_t count, nullmask_t &nullmask,
	                                         SelectionVector *true_sel, SelectionVector *false_sel) {
		if (nullmask.any()) {
			return SelectFlatLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, false>(
			    ldata, rdata, sel, count, nullmask, true_sel, false_sel);
		} else {
			return SelectFlatLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true>(
			    ldata, rdata, sel, count, nullmask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static idx_t SelectFlat(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                        SelectionVector *true_sel, SelectionVector *false_sel) {
		auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
		auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);

		if (LEFT_CONSTANT && ConstantVector::IsNull(left)) {
			return 0;
		}
		if (RIGHT_CONSTANT && ConstantVector::IsNull(right)) {
			return 0;
		}

		if (LEFT_CONSTANT) {
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, FlatVector::Nullmask(right), true_sel, false_sel);
		} else if (RIGHT_CONSTANT) {
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, FlatVector::Nullmask(left), true_sel, false_sel);
		} else {
			auto nullmask = FlatVector::Nullmask(left) | FlatVector::Nullmask(right);
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, nullmask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static inline idx_t
	SelectGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata, const SelectionVector *__restrict lsel,
	                  const SelectionVector *__restrict rsel, const SelectionVector *__restrict result_sel, idx_t count,
	                  nullmask_t &lnullmask, nullmask_t &rnullmask, SelectionVector *true_sel,
	                  SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel->get_index(i);
			auto lindex = lsel->get_index(i);
			auto rindex = rsel->get_index(i);
			if ((NO_NULL || (!lnullmask[lindex] && !rnullmask[rindex])) &&
			    OP::Operation(ldata[lindex], rdata[rindex])) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL>
	static inline idx_t
	SelectGenericLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                           const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
	                           const SelectionVector *__restrict result_sel, idx_t count, nullmask_t &lnullmask,
	                           nullmask_t &rnullmask, SelectionVector *true_sel, SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
		} else if (true_sel) {
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, false>(
			    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
		} else {
			assert(false_sel);
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, false, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static inline idx_t
	SelectGenericLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                        const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
	                        const SelectionVector *__restrict result_sel, idx_t count, nullmask_t &lnullmask,
	                        nullmask_t &rnullmask, SelectionVector *true_sel, SelectionVector *false_sel) {
		if (lnullmask.any() || rnullmask.any()) {
			return SelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, false>(
			    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
		} else {
			return SelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t SelectGeneric(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                           SelectionVector *true_sel, SelectionVector *false_sel) {
		VectorData ldata, rdata;

		left.Orrify(count, ldata);
		right.Orrify(count, rdata);

		return SelectGenericLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP>((LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data,
		                                                          ldata.sel, rdata.sel, sel, count, *ldata.nullmask,
		                                                          *rdata.nullmask, true_sel, false_sel);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel) {
		if (!sel) {
			sel = &FlatVector::IncrementalSelectionVector;
		}
		if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			return SelectConstant<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
		} else if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(left, right, sel, count, true_sel, false_sel);
		} else if (left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(left, right, sel, count, true_sel, false_sel);
		} else if (left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(left, right, sel, count, true_sel, false_sel);
		} else {
			return SelectGeneric<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
		}
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/ternary_executor.hpp
//
//
//===----------------------------------------------------------------------===//







#include <functional>

namespace duckdb {

struct TernaryExecutor {
private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUN>
	static inline void ExecuteLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               RESULT_TYPE *__restrict result_data, idx_t count, const SelectionVector &asel,
	                               const SelectionVector &bsel, const SelectionVector &csel, nullmask_t &anullmask,
	                               nullmask_t &bnullmask, nullmask_t &cnullmask, nullmask_t &result_nullmask, FUN fun) {
		if (anullmask.any() || bnullmask.any() || cnullmask.any()) {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				if (anullmask[aidx] || bnullmask[bidx] || cnullmask[cidx]) {
					result_nullmask[i] = true;
				} else {
					result_data[i] = fun(adata[aidx], bdata[bidx], cdata[cidx]);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				result_data[i] = fun(adata[aidx], bdata[bidx], cdata[cidx]);
			}
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE)>>
	static void Execute(Vector &a, Vector &b, Vector &c, Vector &result, idx_t count, FUN fun) {
		if (a.vector_type == VectorType::CONSTANT_VECTOR && b.vector_type == VectorType::CONSTANT_VECTOR &&
		    c.vector_type == VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::CONSTANT_VECTOR;
			if (ConstantVector::IsNull(a) || ConstantVector::IsNull(b) || ConstantVector::IsNull(c)) {
				ConstantVector::SetNull(result, true);
			} else {
				auto adata = ConstantVector::GetData<A_TYPE>(a);
				auto bdata = ConstantVector::GetData<B_TYPE>(b);
				auto cdata = ConstantVector::GetData<C_TYPE>(c);
				auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
				result_data[0] = fun(*adata, *bdata, *cdata);
			}
		} else {
			result.vector_type = VectorType::FLAT_VECTOR;

			VectorData adata, bdata, cdata;
			a.Orrify(count, adata);
			b.Orrify(count, bdata);
			c.Orrify(count, cdata);

			ExecuteLoop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data,
			    FlatVector::GetData<RESULT_TYPE>(result), count, *adata.sel, *bdata.sel, *cdata.sel, *adata.nullmask,
			    *bdata.nullmask, *cdata.nullmask, FlatVector::Nullmask(result), fun);
		}
	}

private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static inline idx_t SelectLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               const SelectionVector *result_sel, idx_t count, const SelectionVector &asel,
	                               const SelectionVector &bsel, const SelectionVector &csel, nullmask_t &anullmask,
	                               nullmask_t &bnullmask, nullmask_t &cnullmask, SelectionVector *true_sel,
	                               SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel->get_index(i);
			auto aidx = asel.get_index(i);
			auto bidx = bsel.get_index(i);
			auto cidx = csel.get_index(i);
			if ((NO_NULL || (!anullmask[aidx] && !bnullmask[bidx] && !cnullmask[cidx])) &&
			    OP::Operation(adata[aidx], bdata[bidx], cdata[cidx])) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool NO_NULL>
	static inline idx_t SelectLoopSelSwitch(VectorData &adata, VectorData &bdata, VectorData &cdata,
	                                        const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                        SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, true, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, *adata.nullmask, *bdata.nullmask, *cdata.nullmask, true_sel, false_sel);
		} else if (true_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, true, false>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, *adata.nullmask, *bdata.nullmask, *cdata.nullmask, true_sel, false_sel);
		} else {
			assert(false_sel);
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, false, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, *adata.nullmask, *bdata.nullmask, *cdata.nullmask, true_sel, false_sel);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static inline idx_t SelectLoopSwitch(VectorData &adata, VectorData &bdata, VectorData &cdata,
	                                     const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                     SelectionVector *false_sel) {
		if (adata.nullmask->any() || bdata.nullmask->any() || cdata.nullmask->any()) {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, OP, false>(adata, bdata, cdata, sel, count, true_sel,
			                                                              false_sel);
		} else {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, OP, false>(adata, bdata, cdata, sel, count, true_sel,
			                                                              false_sel);
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static idx_t Select(Vector &a, Vector &b, Vector &c, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		if (!sel) {
			sel = &FlatVector::IncrementalSelectionVector;
		}
		VectorData adata, bdata, cdata;
		a.Orrify(count, adata);
		b.Orrify(count, bdata);
		c.Orrify(count, cdata);

		return SelectLoopSwitch<A_TYPE, B_TYPE, C_TYPE, OP>(adata, bdata, cdata, sel, count, true_sel, false_sel);
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_executor.hpp
//
//
//===----------------------------------------------------------------------===//







#include <functional>

namespace duckdb {

struct UnaryOperatorWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, nullmask_t &nullmask, idx_t idx) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct UnaryLambdaWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, nullmask_t &nullmask, idx_t idx) {
		return fun(input);
	}
};

struct UnaryExecutor {
private:
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteLoop(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               const SelectionVector *__restrict sel_vector, nullmask_t &nullmask,
	                               nullmask_t &result_nullmask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (nullmask.any()) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				if (!nullmask[idx]) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
					    fun, ldata[idx], result_nullmask, i);
				} else {
					result_nullmask[i] = true;
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx],
				                                                                                  result_nullmask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteFlat(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               nullmask_t &nullmask, nullmask_t &result_nullmask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (IGNORE_NULL && nullmask.any()) {
			result_nullmask = nullmask;
			for (idx_t i = 0; i < count; i++) {
				if (!nullmask[i]) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
					    fun, ldata[i], result_nullmask, i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				result_data[i] =
				    OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i], result_nullmask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteStandard(Vector &input, Vector &result, idx_t count, FUNC fun) {
		switch (input.vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			result.vector_type = VectorType::CONSTANT_VECTOR;
			auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
			auto ldata = ConstantVector::GetData<INPUT_TYPE>(input);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				*result_data = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
				    fun, *ldata, ConstantVector::Nullmask(result), 0);
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = FlatVector::GetData<INPUT_TYPE>(input);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input));

			ExecuteFlat<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
			    ldata, result_data, count, FlatVector::Nullmask(input), FlatVector::Nullmask(result), fun);
			break;
		}
		default: {
			VectorData vdata;
			input.Orrify(count, vdata);

			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = (INPUT_TYPE *)vdata.data;

			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
			    ldata, result_data, count, vdata.sel, *vdata.nullmask, FlatVector::Nullmask(result), fun);
			break;
		}
		}
	}

public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false,
	          class OPWRAPPER = UnaryOperatorWrapper>
	static void Execute(Vector &input, Vector &result, idx_t count) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool, IGNORE_NULL>(input, result, count, false);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, bool IGNORE_NULL = false,
	          class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapper, bool, FUNC, IGNORE_NULL>(input, result, count,
		                                                                                      fun);
	}
};

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor_state.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
class Expression;
class ExpressionExecutor;
struct ExpressionExecutorState;

struct ExpressionState {
	ExpressionState(Expression &expr, ExpressionExecutorState &root) : expr(expr), root(root) {
	}
	virtual ~ExpressionState() {
	}

	Expression &expr;
	ExpressionExecutorState &root;
	vector<unique_ptr<ExpressionState>> child_states;

public:
	void AddChild(Expression *expr);
};

struct ExpressionExecutorState {
	unique_ptr<ExpressionState> root_state;
	ExpressionExecutor *executor;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_set.hpp
//
//
//===----------------------------------------------------------------------===//



#include <unordered_set>

namespace duckdb {
using std::unordered_set;
}



namespace duckdb {
class CatalogEntry;
class Catalog;
class ClientContext;
class Expression;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class AggregateFunctionSet;
class CopyFunction;
class ScalarFunctionSet;
class ScalarFunction;
class TableFunctionSet;
class TableFunction;

struct FunctionData {
	virtual ~FunctionData() {
	}

	virtual unique_ptr<FunctionData> Copy() {
		return make_unique<FunctionData>();
	};
};

struct TableFunctionData : public FunctionData {
	unique_ptr<FunctionData> Copy() override {
		throw NotImplementedException("Copy not required for table-producing function");
	}
	// used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
	vector<idx_t> column_ids;
};

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	Function(string name) : name(name) {
	}
	virtual ~Function() {
	}

	//! The name of the function
	string name;

public:
	//! Returns the formatted string name(arg1, arg2, ...)
	static string CallToString(string name, vector<LogicalType> arguments);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	static string CallToString(string name, vector<LogicalType> arguments, LogicalType return_type);

	//! Bind a scalar function from the set of functions and input arguments. Returns the index of the chosen function,
	//! or throws an exception if none could be found.
	static idx_t BindFunction(string name, vector<ScalarFunction> &functions, vector<LogicalType> &arguments);
	static idx_t BindFunction(string name, vector<ScalarFunction> &functions,
	                          vector<unique_ptr<Expression>> &arguments);
	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, or throws an exception if none could be found.
	static idx_t BindFunction(string name, vector<AggregateFunction> &functions, vector<LogicalType> &arguments);
	static idx_t BindFunction(string name, vector<AggregateFunction> &functions,
	                          vector<unique_ptr<Expression>> &arguments);
	//! Bind a table function from the set of functions and input arguments. Returns the index of the chosen
	//! function, or throws an exception if none could be found.
	static idx_t BindFunction(string name, vector<TableFunction> &functions, vector<LogicalType> &arguments);
	static idx_t BindFunction(string name, vector<TableFunction> &functions, vector<unique_ptr<Expression>> &arguments);
};

class SimpleFunction : public Function {
public:
	SimpleFunction(string name, vector<LogicalType> arguments, LogicalType varargs = LogicalType::INVALID)
	    : Function(name), arguments(move(arguments)), varargs(varargs) {
	}
	virtual ~SimpleFunction() {
	}

	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The type of varargs to support, or LogicalTypeId::INVALID if the function does not accept variable length
	//! arguments
	LogicalType varargs;

public:
	virtual string ToString() {
		return Function::CallToString(name, arguments);
	}

	bool HasVarArgs() {
		return varargs.id() != LogicalTypeId::INVALID;
	}
};

class BaseScalarFunction : public SimpleFunction {
public:
	BaseScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type, bool has_side_effects,
	                   LogicalType varargs = LogicalType::INVALID)
	    : SimpleFunction(move(name), move(arguments), move(varargs)), return_type(return_type),
	      has_side_effects(has_side_effects) {
	}
	virtual ~BaseScalarFunction() {
	}

	//! Return type of the function
	LogicalType return_type;
	//! Whether or not the function has side effects (e.g. sequence increments, random() functions, NOW()). Functions
	//! with side-effects cannot be constant-folded.
	bool has_side_effects;

public:
	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(vector<unique_ptr<Expression>> &children);

	string ToString() override {
		return Function::CallToString(name, arguments, return_type);
	}
};

class BuiltinFunctions {
public:
	BuiltinFunctions(ClientContext &transaction, Catalog &catalog);

	//! Initialize a catalog with all built-in functions
	void Initialize();

public:
	void AddFunction(AggregateFunctionSet set);
	void AddFunction(AggregateFunction function);
	void AddFunction(ScalarFunctionSet set);
	void AddFunction(ScalarFunction function);
	void AddFunction(vector<string> names, ScalarFunction function);
	void AddFunction(TableFunctionSet set);
	void AddFunction(TableFunction function);
	void AddFunction(CopyFunction function);

	void AddCollation(string name, ScalarFunction function, bool combinable = false,
	                  bool not_required_for_equality = false);

private:
	ClientContext &context;
	Catalog &catalog;

private:
	template <class T> void Register() {
		T::RegisterFunction(*this);
	}

	// table-producing functions
	void RegisterSQLiteFunctions();
	void RegisterReadFunctions();
	void RegisterTableFunctions();
	void RegisterArrowFunctions();

	// aggregates
	void RegisterAlgebraicAggregates();
	void RegisterDistributiveAggregates();
	void RegisterNestedAggregates();

	// scalar functions
	void RegisterDateFunctions();
	void RegisterGenericFunctions();
	void RegisterMathFunctions();
	void RegisterOperators();
	void RegisterStringFunctions();
	void RegisterNestedFunctions();
	void RegisterSequenceFunctions();
	void RegisterTrigonometricsFunctions();
};

} // namespace duckdb


namespace duckdb {
class BoundFunctionExpression;
class ScalarFunctionCatalogEntry;

//! The type used for scalar functions
typedef std::function<void(DataChunk &, ExpressionState &, Vector &)> scalar_function_t;
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_scalar_function_t)(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments);
//! Adds the dependencies of this BoundFunctionExpression to the set of dependencies
typedef void (*dependency_function_t)(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies);

class ScalarFunction : public BaseScalarFunction {
public:
	ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr, LogicalType varargs = LogicalType::INVALID)
	    : BaseScalarFunction(name, arguments, return_type, has_side_effects, varargs), function(function), bind(bind),
	      dependency(dependency) {
	}

	ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr, LogicalType varargs = LogicalType::INVALID)
	    : ScalarFunction(string(), arguments, return_type, function, has_side_effects, bind, dependency, varargs) {
	}

	//! The main scalar function to execute
	scalar_function_t function;
	//! The bind function (if any)
	bind_scalar_function_t bind;
	// The dependency function (if any)
	dependency_function_t dependency;

	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context, string schema, string name,
	                                                              vector<unique_ptr<Expression>> children,
	                                                              bool is_operator = false);
	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context,
	                                                              ScalarFunctionCatalogEntry &function,
	                                                              vector<unique_ptr<Expression>> children,
	                                                              bool is_operator = false);

	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context, ScalarFunction bound_function,
	                                                              vector<unique_ptr<Expression>> children,
	                                                              bool is_operator = false);

	bool operator==(const ScalarFunction &rhs) const {
		return CompareScalarFunctionT(rhs.function) && bind == rhs.bind && dependency == rhs.dependency;
	}
	bool operator!=(const ScalarFunction &rhs) const {
		return !(*this == rhs);
	}

private:
	bool CompareScalarFunctionT(const scalar_function_t other) const {
		typedef void(funcTypeT)(DataChunk &, ExpressionState &, Vector &);

		funcTypeT **func_ptr = (funcTypeT **)function.template target<funcTypeT *>();
		funcTypeT **other_ptr = (funcTypeT **)other.template target<funcTypeT *>();

		// Case the functions were created from lambdas the target will return a nullptr
		if (func_ptr == nullptr || other_ptr == nullptr) {
			// scalar_function_t (std::functions) from lambdas cannot be compared
			return false;
		}
		return ((size_t)*func_ptr == (size_t)*other_ptr);
	}

public:
	static void NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count() >= 1);
		result.Reference(input.data[0]);
	}

	template <class TA, class TR, class OP, bool SKIP_NULLS = false>
	static void UnaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count() >= 1);
		UnaryExecutor::Execute<TA, TR, OP, SKIP_NULLS>(input.data[0], result, input.size());
	}

	template <class TA, class TB, class TR, class OP, bool IGNORE_NULL = false>
	static void BinaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count() == 2);
		BinaryExecutor::ExecuteStandard<TA, TB, TR, OP, IGNORE_NULL>(input.data[0], input.data[1], result,
		                                                             input.size());
	}

public:
	template <class OP> static scalar_function_t GetScalarUnaryFunction(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, int8_t, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, int16_t, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, int32_t, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, int64_t, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, hugeint_t, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, float, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, double, OP>;
			break;
		default:
			throw NotImplementedException("Unimplemented type for GetScalarUnaryFunction");
		}
		return function;
	}

	template <class TR, class OP> static scalar_function_t GetScalarUnaryFunctionFixedReturn(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, TR, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, TR, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, TR, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, TR, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, TR, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, TR, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, TR, OP>;
			break;
		default:
			throw NotImplementedException("Unimplemented type for GetScalarUnaryFunctionFixedReturn");
		}
		return function;
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/aggregate_executor.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {

class AggregateExecutor {
private:
	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatLoop(INPUT_TYPE *__restrict idata, STATE_TYPE **__restrict states, nullmask_t &nullmask,
	                                 idx_t count) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				if (!nullmask[i]) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], idata, nullmask, i);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], idata, nullmask, i);
			}
		}
	}
	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryScatterLoop(INPUT_TYPE *__restrict idata, STATE_TYPE **__restrict states,
	                                    const SelectionVector &isel, const SelectionVector &ssel, nullmask_t &nullmask,
	                                    idx_t count) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (!nullmask[idx]) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[sidx], idata, nullmask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[sidx], idata, nullmask, idx);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP, bool HAS_SEL_VECTOR>
	static inline void UnaryUpdateLoop(INPUT_TYPE *__restrict idata, STATE_TYPE *__restrict state, idx_t count,
	                                   nullmask_t &nullmask, const SelectionVector *__restrict sel_vector) {
		if (OP::IgnoreNull() && nullmask.any()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? sel_vector->get_index(i) : i;
				if (!nullmask[idx]) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, nullmask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? sel_vector->get_index(i) : i;
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, idata, nullmask, idx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryScatterLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata,
	                                     STATE_TYPE **__restrict states, idx_t count, const SelectionVector &asel,
	                                     const SelectionVector &bsel, const SelectionVector &ssel,
	                                     nullmask_t &anullmask, nullmask_t &bnullmask) {
		if (OP::IgnoreNull() && (anullmask.any() || bnullmask.any())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (!anullmask[aidx] && !bnullmask[bidx]) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[sidx], adata, bdata, anullmask,
					                                                       bnullmask, aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[sidx], adata, bdata, anullmask, bnullmask,
				                                                       aidx, bidx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryUpdateLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata,
	                                    STATE_TYPE *__restrict state, idx_t count, const SelectionVector &asel,
	                                    const SelectionVector &bsel, nullmask_t &anullmask, nullmask_t &bnullmask) {
		if (OP::IgnoreNull() && (anullmask.any() || bnullmask.any())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				if (!anullmask[aidx] && !bnullmask[bidx]) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, adata, bdata, anullmask, bnullmask,
					                                                       aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, adata, bdata, anullmask, bnullmask, aidx,
				                                                       bidx);
			}
		}
	}

public:
	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryScatter(Vector &input, Vector &states, idx_t count) {
		if (input.vector_type == VectorType::CONSTANT_VECTOR && states.vector_type == VectorType::CONSTANT_VECTOR) {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(*sdata, idata, ConstantVector::Nullmask(input),
			                                                           count);
		} else if (input.vector_type == VectorType::FLAT_VECTOR && states.vector_type == VectorType::FLAT_VECTOR) {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			UnaryFlatLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, sdata, FlatVector::Nullmask(input), count);
		} else {
			VectorData idata, sdata;
			input.Orrify(count, idata);
			states.Orrify(count, sdata);
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP>((INPUT_TYPE *)idata.data, (STATE_TYPE **)sdata.data,
			                                             *idata.sel, *sdata.sel, *idata.nullmask, count);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector &input, data_ptr_t state, idx_t count) {
		switch (input.vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				return;
			}
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>((STATE_TYPE *)state, idata,
			                                                           ConstantVector::Nullmask(input), count);
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP, false>(idata, (STATE_TYPE *)state, count,
			                                                   FlatVector::Nullmask(input), nullptr);
			break;
		}
		default: {
			VectorData idata;
			input.Orrify(count, idata);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP, true>((INPUT_TYPE *)idata.data, (STATE_TYPE *)state, count,
			                                                  *idata.nullmask, idata.sel);
			break;
		}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(Vector &a, Vector &b, Vector &states, idx_t count) {
		VectorData adata, bdata, sdata;

		a.Orrify(count, adata);
		b.Orrify(count, bdata);
		states.Orrify(count, sdata);

		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE *)adata.data, (B_TYPE *)bdata.data,
		                                                  (STATE_TYPE **)sdata.data, count, *adata.sel, *bdata.sel,
		                                                  *sdata.sel, *adata.nullmask, *bdata.nullmask);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector &a, Vector &b, data_ptr_t state, idx_t count) {
		VectorData adata, bdata;

		a.Orrify(count, adata);
		b.Orrify(count, bdata);

		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE *)adata.data, (B_TYPE *)bdata.data,
		                                                 (STATE_TYPE *)state, count, *adata.sel, *bdata.sel,
		                                                 *adata.nullmask, *bdata.nullmask);
	}

	template <class STATE_TYPE, class OP> static void Combine(Vector &source, Vector &target, idx_t count) {
		assert(source.type.id() == LogicalTypeId::POINTER && target.type.id() == LogicalTypeId::POINTER);
		auto sdata = FlatVector::GetData<STATE_TYPE *>(source);
		auto tdata = FlatVector::GetData<STATE_TYPE *>(target);

		for (idx_t i = 0; i < count; i++) {
			OP::template Combine<STATE_TYPE, OP>(*sdata[i], tdata[i]);
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE, class OP>
	static void Finalize(Vector &states, Vector &result, idx_t count) {
		if (states.vector_type == VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::CONSTANT_VECTOR;

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, *sdata, rdata, ConstantVector::Nullmask(result), 0);
		} else {
			assert(states.vector_type == VectorType::FLAT_VECTOR);
			result.vector_type = VectorType::FLAT_VECTOR;

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
			for (idx_t i = 0; i < count; i++) {
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[i], rdata, FlatVector::Nullmask(result),
				                                               i);
			}
		}
	}

	template <class STATE_TYPE, class OP> static void Destroy(Vector &states, idx_t count) {
		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		for (idx_t i = 0; i < count; i++) {
			OP::template Destroy<STATE_TYPE>(sdata[i]);
		}
	}
};

} // namespace duckdb


namespace duckdb {

class BoundAggregateExpression;

//! The type used for sizing hashed aggregate function states
typedef idx_t (*aggregate_size_t)();
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(data_ptr_t state);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], idx_t input_count, Vector &state, idx_t count);
//! The type used for combining hashed aggregate states (optional)
typedef void (*aggregate_combine_t)(Vector &state, Vector &combined, idx_t count);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &state, Vector &result, idx_t count);
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_aggregate_function_t)(ClientContext &context, AggregateFunction &function,
                                                              vector<unique_ptr<Expression>> &arguments);
//! The type used for the aggregate destructor method. NOTE: this method is used in destructors and MAY NOT throw.
typedef void (*aggregate_destructor_t)(Vector &state, idx_t count);

//! The type used for updating simple (non-grouped) aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], idx_t input_count, data_ptr_t state, idx_t count);

class AggregateFunction : public BaseScalarFunction {
public:
	AggregateFunction(string name, vector<LogicalType> arguments, LogicalType return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, false), state_size(state_size), initialize(initialize),
	      update(update), combine(combine), finalize(finalize), simple_update(simple_update), bind(bind),
	      destructor(destructor) {
	}

	AggregateFunction(vector<LogicalType> arguments, LogicalType return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        simple_update, bind, destructor) {
	}

	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate state initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update state function
	aggregate_update_t update;
	//! The hashed aggregate combine states function
	aggregate_combine_t combine;
	//! The hashed aggregate finalization function
	aggregate_finalize_t finalize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;

	//! The bind function (may be null)
	bind_aggregate_function_t bind;
	//! The destructor method (may be null)
	aggregate_destructor_t destructor;

	bool operator==(const AggregateFunction &rhs) const {
		return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
		       combine == rhs.combine && finalize == rhs.finalize;
	}
	bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}

	static unique_ptr<BoundAggregateExpression> BindAggregateFunction(ClientContext &context,
	                                                                  AggregateFunction bound_function,
	                                                                  vector<unique_ptr<Expression>> children,
	                                                                  bool is_distinct = false);

public:
	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction UnaryAggregate(LogicalType input_type, LogicalType return_type) {
		return AggregateFunction(
		    {input_type}, return_type, AggregateFunction::StateSize<STATE>,
		    AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
		    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		    AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction UnaryAggregateDestructor(LogicalType input_type, LogicalType return_type) {
		auto aggregate = UnaryAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP>(input_type, return_type);
		aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	}

	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction BinaryAggregate(LogicalType a_type, LogicalType b_type, LogicalType return_type) {
		return AggregateFunction({a_type, b_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP>,
		                         AggregateFunction::BinaryScatterUpdate<STATE, A_TYPE, B_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		                         AggregateFunction::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>);
	}

public:
	template <class STATE> static idx_t StateSize() {
		return sizeof(STATE);
	}

	template <class STATE, class OP> static void StateInitialize(data_ptr_t state) {
		OP::Initialize((STATE *)state);
	}

	template <class STATE, class T, class OP>
	static void UnaryScatterUpdate(Vector inputs[], idx_t input_count, Vector &states, idx_t count) {
		assert(input_count == 1);
		AggregateExecutor::UnaryScatter<STATE, T, OP>(inputs[0], states, count);
	}

	template <class STATE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector inputs[], idx_t input_count, data_ptr_t state, idx_t count) {
		assert(input_count == 1);
		AggregateExecutor::UnaryUpdate<STATE, INPUT_TYPE, OP>(inputs[0], state, count);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatterUpdate(Vector inputs[], idx_t input_count, Vector &states, idx_t count) {
		assert(input_count == 2);
		AggregateExecutor::BinaryScatter<STATE, A_TYPE, B_TYPE, OP>(inputs[0], inputs[1], states, count);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector inputs[], idx_t input_count, data_ptr_t state, idx_t count) {
		assert(input_count == 2);
		AggregateExecutor::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>(inputs[0], inputs[1], state, count);
	}

	template <class STATE, class OP> static void StateCombine(Vector &source, Vector &target, idx_t count) {
		AggregateExecutor::Combine<STATE, OP>(source, target, count);
	}

	template <class STATE, class RESULT_TYPE, class OP>
	static void StateFinalize(Vector &states, Vector &result, idx_t count) {
		AggregateExecutor::Finalize<STATE, RESULT_TYPE, OP>(states, result, count);
	}

	template <class STATE, class OP> static void StateDestroy(Vector &states, idx_t count) {
		AggregateExecutor::Destroy<STATE, OP>(states, count);
	}
};

} // namespace duckdb


using namespace std;

namespace duckdb {

struct UDFWrapper {
public:
	template <typename TR, typename... Args>
	static scalar_function_t CreateScalarFunction(string name, TR (*udf_func)(Args...)) {
		const std::size_t num_template_argc = sizeof...(Args);
		switch (num_template_argc) {
		case 1:
			return CreateUnaryFunction<TR, Args...>(name, udf_func);
		case 2:
			return CreateBinaryFunction<TR, Args...>(name, udf_func);
		case 3:
			return CreateTernaryFunction<TR, Args...>(name, udf_func);
		default:
			throw duckdb::NotImplementedException("UDF function only supported until ternary!");
		}
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateScalarFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                              TR (*udf_func)(Args...)) {
		if (!TypesMatch<TR>(ret_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TR>(), ret_type.InternalType(),
			                                    "Return type doesn't match with the first template type.");
		}

		const std::size_t num_template_types = sizeof...(Args);
		if (num_template_types != args.size()) {
			throw duckdb::InvalidInputException(
			    "The number of templated types should be the same quantity of the LogicalType arguments.");
		}

		switch (num_template_types) {
		case 1:
			return CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
		case 2:
			return CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
		case 3:
			return CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
		default:
			throw duckdb::NotImplementedException("UDF function only supported until ternary!");
		}
	}

	template <typename TR, typename... Args>
	static void RegisterFunction(string name, scalar_function_t udf_function, ClientContext &context,
	                             LogicalType varargs = LogicalType::INVALID) {
		vector<LogicalType> arguments;
		GetArgumentTypesRecursive<Args...>(arguments);

		LogicalType ret_type = GetArgumentType<TR>();

		RegisterFunction(name, arguments, ret_type, udf_function, context, varargs);
	}

	static void RegisterFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                             scalar_function_t udf_function, ClientContext &context,
	                             LogicalType varargs = LogicalType::INVALID);

	//--------------------------------- Aggregate UDFs ------------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateAggregateFunction(string name) {
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateAggregateFunction(string name) {
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateAggregateFunction(string name, LogicalType ret_type, LogicalType input_type) {
		if (!TypesMatch<TR>(ret_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TR>(), ret_type.InternalType(),
			                                    "The return argument don't match!");
		}

		if (!TypesMatch<TA>(input_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), input_type.InternalType(),
			                                    "The input argument don't match!");
		}

		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, ret_type, input_type);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateAggregateFunction(string name, LogicalType ret_type, LogicalType input_typeA,
	                                                 LogicalType input_typeB) {
		if (!TypesMatch<TR>(ret_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TR>(), ret_type.InternalType(),
			                                    "The return argument don't match!");
		}

		if (!TypesMatch<TA>(input_typeA)) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), input_typeA.InternalType(),
			                                    "The first input argument don't match!");
		}

		if (!TypesMatch<TB>(input_typeB)) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), input_typeB.InternalType(),
			                                    "The second input argument don't match!");
		}

		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, ret_type, input_typeA, input_typeB);
	}

	//! A generic CreateAggregateFunction ---------------------------------------------------------------------------//
	static AggregateFunction CreateAggregateFunction(string name, vector<LogicalType> arguments,
	                                                 LogicalType return_type, aggregate_size_t state_size,
	                                                 aggregate_initialize_t initialize, aggregate_update_t update,
	                                                 aggregate_combine_t combine, aggregate_finalize_t finalize,
	                                                 aggregate_simple_update_t simple_update = nullptr,
	                                                 bind_aggregate_function_t bind = nullptr,
	                                                 aggregate_destructor_t destructor = nullptr) {

		AggregateFunction aggr_function(name, arguments, return_type, state_size, initialize, update, combine, finalize,
		                                simple_update, bind, destructor);
		return aggr_function;
	}

	static void RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context,
	                                 LogicalType varargs = LogicalType::INVALID);

private:
	//-------------------------------- Templated functions --------------------------------//
	template <typename TR, typename... Args>
	static scalar_function_t CreateUnaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 1);
		return CreateUnaryFunction<TR, Args...>(name, udf_func);
	}

	template <typename TR, typename TA> static scalar_function_t CreateUnaryFunction(string name, TR (*udf_func)(TA)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			UnaryExecutor::Execute<TA, TR>(input.data[0], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateBinaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 2);
		return CreateBinaryFunction<TR, Args...>(name, udf_func);
	}

	template <typename TR, typename TA, typename TB>
	static scalar_function_t CreateBinaryFunction(string name, TR (*udf_func)(TA, TB)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			BinaryExecutor::Execute<TA, TB, TR>(input.data[0], input.data[1], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateTernaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 3);
		return CreateTernaryFunction<TR, Args...>(name, udf_func);
	}

	template <typename TR, typename TA, typename TB, typename TC>
	static scalar_function_t CreateTernaryFunction(string name, TR (*udf_func)(TA, TB, TC)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0], input.data[1], input.data[2], result, input.size(),
			                                         udf_func);
		};
		return udf_function;
	}

	template <typename T> static LogicalType GetArgumentType() {
		if (std::is_same<T, bool>()) {
			return LogicalType::BOOLEAN;
		} else if (std::is_same<T, int8_t>()) {
			return LogicalType::TINYINT;
		} else if (std::is_same<T, int16_t>()) {
			return LogicalType::SMALLINT;
		} else if (std::is_same<T, int32_t>()) {
			return LogicalType::INTEGER;
		} else if (std::is_same<T, int64_t>()) {
			return LogicalType::BIGINT;
		} else if (std::is_same<T, float>()) {
			return LogicalType::FLOAT;
		} else if (std::is_same<T, double>()) {
			return LogicalType::DOUBLE;
		} else if (std::is_same<T, string_t>()) {
			return LogicalType::VARCHAR;
		} else {
			// unrecognized type
			throw duckdb::InternalException("Unrecognized type!");
		}
	}

	template <typename TA, typename TB, typename... Args>
	static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
		GetArgumentTypesRecursive<TB, Args...>(arguments);
	}

	template <typename TA> static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
	}

private:
	//-------------------------------- Argumented functions --------------------------------//

	template <typename TR, typename... Args>
	static scalar_function_t CreateUnaryFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                             TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 1);
		return CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template <typename TR, typename TA>
	static scalar_function_t CreateUnaryFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                             TR (*udf_func)(TA)) {
		if (args.size() != 1) {
			throw duckdb::InvalidInputException("The number of LogicalType arguments (\"args\") should be 1!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), args[0].InternalType(),
			                                    "The first arguments don't match!");
		}

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			UnaryExecutor::Execute<TA, TR>(input.data[0], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateBinaryFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                              TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 2);
		return CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template <typename TR, typename TA, typename TB>
	static scalar_function_t CreateBinaryFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                              TR (*udf_func)(TA, TB)) {
		if (args.size() != 2) {
			throw duckdb::InvalidInputException("The number of LogicalType arguments (\"args\") should be 2!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), args[0].InternalType(),
			                                    "The first arguments don't match!");
		}
		if (!TypesMatch<TB>(args[1])) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), args[1].InternalType(),
			                                    "The second arguments don't match!");
		}

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) {
			BinaryExecutor::Execute<TA, TB, TR>(input.data[0], input.data[1], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateTernaryFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                               TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 3);
		return CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template <typename TR, typename TA, typename TB, typename TC>
	static scalar_function_t CreateTernaryFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                               TR (*udf_func)(TA, TB, TC)) {
		if (args.size() != 3) {
			throw duckdb::InvalidInputException("The number of LogicalType arguments (\"args\") should be 3!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), args[0].InternalType(),
			                                    "The first arguments don't match!");
		}
		if (!TypesMatch<TB>(args[1])) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), args[1].InternalType(),
			                                    "The second arguments don't match!");
		}
		if (!TypesMatch<TC>(args[2])) {
			throw duckdb::TypeMismatchException(GetTypeId<TC>(), args[2].InternalType(),
			                                    "The second arguments don't match!");
		}

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0], input.data[1], input.data[2], result, input.size(),
			                                         udf_func);
		};
		return udf_function;
	}

	template <typename T> static bool TypesMatch(LogicalType sql_type) {
		switch (sql_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return std::is_same<T, bool>();
		case LogicalTypeId::TINYINT:
			return std::is_same<T, int8_t>();
		case LogicalTypeId::SMALLINT:
			return std::is_same<T, int16_t>();
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::INTEGER:
			return std::is_same<T, int32_t>();
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::TIMESTAMP:
			return std::is_same<T, int64_t>();
		case LogicalTypeId::FLOAT:
			return std::is_same<T, float>();
		case LogicalTypeId::DOUBLE:
			return std::is_same<T, double>();
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR:
		case LogicalTypeId::BLOB:
			return std::is_same<T, string_t>();
		case LogicalTypeId::VARBINARY:
			return std::is_same<T, blob_t>();
		default:
			throw InvalidTypeException(sql_type.InternalType(), "Type does not supported!");
		}
	}

private:
	//-------------------------------- Aggregate functions --------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateUnaryAggregateFunction(string name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_type = GetArgumentType<TA>();
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, return_type, input_type);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateUnaryAggregateFunction(string name, LogicalType ret_type, LogicalType input_type) {
		AggregateFunction aggr_function =
		    AggregateFunction::UnaryAggregate<STATE, TR, TA, UDF_OP>(input_type, ret_type);
		aggr_function.name = name;
		return aggr_function;
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateBinaryAggregateFunction(string name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_typeA = GetArgumentType<TA>();
		LogicalType input_typeB = GetArgumentType<TB>();
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, return_type, input_typeA, input_typeB);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateBinaryAggregateFunction(string name, LogicalType ret_type, LogicalType input_typeA,
	                                                       LogicalType input_typeB) {
		AggregateFunction aggr_function =
		    AggregateFunction::BinaryAggregate<STATE, TR, TA, TB, UDF_OP>(input_typeA, input_typeB, ret_type);
		aggr_function.name = name;
		return aggr_function;
	}
}; // end UDFWrapper

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {

//! The Serialize class is a base class that can be used to serializing objects into a binary buffer
class Serializer {
public:
	virtual ~Serializer() {
	}

	virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;


	template <class T> void Write(T element) {
		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	//! Write data from a string buffer directly (wihtout length prefix)
	void WriteBufferData(const string &str) {
		WriteData((const_data_ptr_t) str.c_str(), str.size());
	}
	//! Write a string with a length prefix
	void WriteString(const string &val) {
		Write<uint32_t>((uint32_t)val.size());
		if (val.size() > 0) {
			WriteData((const_data_ptr_t)val.c_str(), val.size());
		}
	}

	template <class T> void WriteList(vector<unique_ptr<T>> &list) {
		Write<uint32_t>((uint32_t)list.size());
		for (auto &child : list) {
			child->Serialize(*this);
		}
	}

	template <class T> void WriteOptional(unique_ptr<T> &element) {
		Write<bool>(element ? true : false);
		if (element) {
			element->Serialize(*this);
		}
	}
};

//! The Deserializer class assists in deserializing a binary blob back into an
//! object
class Deserializer {
public:
	virtual ~Deserializer() {
	}

	//! Reads [read_size] bytes into the buffer
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

	template <class T> T Read() {
		T value;
		ReadData((data_ptr_t)&value, sizeof(T));
		return value;
	}

	template <class T> void ReadList(vector<unique_ptr<T>> &list) {
		auto select_count = Read<uint32_t>();
		for (uint32_t i = 0; i < select_count; i++) {
			auto child = T::Deserialize(*this);
			list.push_back(move(child));
		}
	}

	template <class T> unique_ptr<T> ReadOptional() {
		auto has_entry = Read<bool>();
		if (has_entry) {
			return T::Deserialize(*this);
		}
		return nullptr;
	}
};

template <> string Deserializer::Read();

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_buffer.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {
struct FileHandle;

enum class FileBufferType : uint8_t { BLOCK = 1, MANAGED_BUFFER = 2 };

//! The FileBuffer represents a buffer that can be read or written to a Direct IO FileHandle.
class FileBuffer {
public:
	//! Allocates a buffer of the specified size that is sector-aligned. bufsiz must be a multiple of
	//! FileSystemConstants::FILE_BUFFER_BLOCK_SIZE. The content in this buffer can be written to FileHandles that have
	//! been opened with DIRECT_IO on all operating systems, however, the entire buffer must be written to the file.
	//! Note that the returned size is 8 bytes less than the allocation size to account for the checksum.
	FileBuffer(FileBufferType type, uint64_t bufsiz);
	virtual ~FileBuffer();

	//! The type of the buffer
	FileBufferType type;
	//! The buffer that users can write to
	data_ptr_t buffer;
	//! The size of the portion that users can write to, this is equivalent to internal_size - BLOCK_HEADER_SIZE
	uint64_t size;

public:
	//! Read into the FileBuffer from the specified location. Automatically verifies the checksum, and throws an
	//! exception if the checksum does not match correctly.
	void Read(FileHandle &handle, uint64_t location);
	//! Write the contents of the FileBuffer to the specified location. Automatically adds a checksum of the contents of
	//! the filebuffer in front of the written data.
	void Write(FileHandle &handle, uint64_t location);

	void Clear();

	uint64_t AllocSize() {
		return internal_size;
	}

private:
	//! The pointer to the internal buffer that will be read or written, including the buffer header
	data_ptr_t internal_buffer;
	//! The aligned size as passed to the constructor. This is the size that is read or written to disk.
	uint64_t internal_size;

	//! The buffer that was actually malloc'd, i.e. the pointer that must be freed when the FileBuffer is destroyed
	data_ptr_t malloced_buffer;
};

} // namespace duckdb


#include <functional>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

namespace duckdb {
class ClientContext;
class FileSystem;

struct FileHandle {
public:
	FileHandle(FileSystem &file_system, string path) : file_system(file_system), path(path) {
	}
	FileHandle(const FileHandle &) = delete;
	virtual ~FileHandle() {
	}

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);
	void Sync();
	void Truncate(int64_t new_size);

protected:
	virtual void Close() = 0;

public:
	FileSystem &file_system;
	string path;
};

enum class FileLockType : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

class FileFlags {
public:
	//! Open file with read access
	static constexpr uint8_t FILE_FLAGS_READ = 1 << 0;
	//! Open file with read/write access
	static constexpr uint8_t FILE_FLAGS_WRITE = 1 << 1;
	//! Use direct IO when reading/writing to the file
	static constexpr uint8_t FILE_FLAGS_DIRECT_IO = 1 << 2;
	//! Create file if not exists, can only be used together with WRITE
	static constexpr uint8_t FILE_FLAGS_FILE_CREATE = 1 << 3;
	//! Always create a new file. If a file exists, the file is truncated. Cannot be used together with CREATE.
	static constexpr uint8_t FILE_FLAGS_FILE_CREATE_NEW = 1 << 4;
	//! Open file in append mode
	static constexpr uint8_t FILE_FLAGS_APPEND = 1 << 5;
};

class FileSystem {
public:
	virtual ~FileSystem() {
	}

public:
	static FileSystem &GetFileSystem(ClientContext &context);

	virtual unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK);
	unique_ptr<FileHandle> OpenFile(string &path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK) {
		return OpenFile(path.c_str(), flags, lock);
	}
	//! Read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Read().
	virtual void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Write().
	virtual void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
	//! amount of bytes read.
	virtual int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	//! Write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
	virtual int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);

	//! Returns the file size of a file handle, returns -1 on error
	virtual int64_t GetFileSize(FileHandle &handle);
	//! Truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
	//! the file
	virtual void Truncate(FileHandle &handle, int64_t new_size);

	//! Check if a directory exists
	virtual bool DirectoryExists(const string &directory);
	//! Create a directory if it does not exist
	virtual void CreateDirectory(const string &directory);
	//! Recursively remove a directory and all files in it
	virtual void RemoveDirectory(const string &directory);
	//! List files in a directory, invoking the callback method for each one with (filename, is_dir)
	virtual bool ListFiles(const string &directory, std::function<void(string, bool)> callback);
	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	virtual void MoveFile(const string &source, const string &target);
	//! Check if a file exists
	virtual bool FileExists(const string &filename);
	//! Remove a file from disk
	virtual void RemoveFile(const string &filename);
	//! Path separator for the current file system
	virtual string PathSeparator();
	//! Join two paths together
	virtual string JoinPath(const string &a, const string &path);
	//! Sync a file handle to disk
	virtual void FileSync(FileHandle &handle);

	//! Sets the working directory
	virtual void SetWorkingDirectory(string path);

private:
	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	void SetFilePointer(FileHandle &handle, idx_t location);
};

} // namespace duckdb


namespace duckdb {

#define FILE_BUFFER_SIZE 4096

class BufferedFileWriter : public Serializer {
public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	BufferedFileWriter(FileSystem &fs, string path, uint8_t open_flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);

	FileSystem &fs;
	unique_ptr<data_t[]> data;
	idx_t offset;
	idx_t total_written;
	unique_ptr<FileHandle> handle;

public:
	void WriteData(const_data_ptr_t buffer, uint64_t write_size) override;
	//! Flush the buffer to disk and sync the file to ensure writing is completed
	void Sync();
	//! Flush the buffer to the file (without sync)
	void Flush();
	//! Returns the current size of the file
	int64_t GetFileSize();
	//! Truncate the size to a previous size (given that size <= GetFileSize())
	void Truncate(int64_t size);

	idx_t GetTotalWritten();
};

} // namespace duckdb


namespace duckdb {

class ClientContext;
class DuckDB;

typedef void (*warning_callback)(std::string);

//! A connection to a database. This represents a (client) connection that can
//! be used to query the database.
class Connection {
public:
	Connection(DuckDB &database);
	~Connection();

	DuckDB &db;
	unique_ptr<ClientContext> context;
	warning_callback warning_cb;

public:
	//! Returns query profiling information for the current query
	string GetProfilingInformation(ProfilerPrintFormat format = ProfilerPrintFormat::QUERY_TREE);

	//! Interrupt execution of the current query
	void Interrupt();

	//! Enable query profiling
	void EnableProfiling();
	//! Disable query profiling
	void DisableProfiling();

	void SetWarningCallback(warning_callback);

	//! Enable aggressive verification/testing of queries, should only be used in testing
	void EnableQueryVerification();
	void DisableQueryVerification();
	//! Force parallel execution, even for smaller tables. Should only be used in testing.
	void ForceParallelism();

	//! Issues a query to the database and returns a QueryResult. This result can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The result can be stepped through with calls to Fetch(). Note that there can only be
	//! one active StreamQueryResult per Connection object. Calling SendQuery() will invalidate any previously existing
	//! StreamQueryResult.
	unique_ptr<QueryResult> SendQuery(string query);
	//! Issues a query to the database and materializes the result (if necessary). Always returns a
	//! MaterializedQueryResult.
	unique_ptr<MaterializedQueryResult> Query(string query);
	// prepared statements
	template <typename... Args> unique_ptr<QueryResult> Query(string query, Args... args) {
		vector<Value> values;
		return QueryParamsRecursive(query, values, args...);
	}

	//! Prepare the specified query, returning a prepared statement object
	unique_ptr<PreparedStatement> Prepare(string query);

	//! Get the table info of a specific table (in the default schema), or nullptr if it cannot be found
	unique_ptr<TableDescription> TableInfo(string table_name);
	//! Get the table info of a specific table, or nullptr if it cannot be found
	unique_ptr<TableDescription> TableInfo(string schema_name, string table_name);

	//! Extract a set of SQL statements from a specific query
	vector<unique_ptr<SQLStatement>> ExtractStatements(string query);

	//! Appends a DataChunk to the specified table
	void Append(TableDescription &description, DataChunk &chunk);

	//! Returns a relation that produces a table from this connection
	shared_ptr<Relation> Table(string tname);
	shared_ptr<Relation> Table(string schema_name, string table_name);
	//! Returns a relation that produces a view from this connection
	shared_ptr<Relation> View(string tname);
	shared_ptr<Relation> View(string schema_name, string table_name);
	//! Returns a relation that calls a specified table function
	shared_ptr<Relation> TableFunction(string tname);
	shared_ptr<Relation> TableFunction(string tname, vector<Value> values);
	//! Returns a relation that produces values
	shared_ptr<Relation> Values(vector<vector<Value>> values);
	shared_ptr<Relation> Values(vector<vector<Value>> values, vector<string> column_names, string alias = "values");
	shared_ptr<Relation> Values(string values);
	shared_ptr<Relation> Values(string values, vector<string> column_names, string alias = "values");
	//! Reads CSV file
	shared_ptr<Relation> ReadCSV(string csv_file, vector<string> columns);

	void BeginTransaction();
	void Commit();
	void Rollback();

	template <typename TR, typename... Args> void CreateScalarFunction(string name, TR (*udf_func)(Args...)) {
		scalar_function_t function = UDFWrapper::CreateScalarFunction<TR, Args...>(name, udf_func);
		UDFWrapper::RegisterFunction<TR, Args...>(name, function, *context);
	}

	template <typename TR, typename... Args>
	void CreateScalarFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(Args...)) {
		scalar_function_t function = UDFWrapper::CreateScalarFunction<TR, Args...>(name, args, ret_type, udf_func);
		UDFWrapper::RegisterFunction(name, args, ret_type, function, *context);
	}

	template <typename TR, typename... Args>
	void CreateVectorizedFunction(string name, scalar_function_t udf_func, LogicalType varargs = LogicalType::INVALID) {
		UDFWrapper::RegisterFunction<TR, Args...>(name, udf_func, *context, varargs);
	}

	void CreateVectorizedFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                              scalar_function_t udf_func, LogicalType varargs = LogicalType::INVALID) {
		UDFWrapper::RegisterFunction(name, args, ret_type, udf_func, *context, varargs);
	}

	//------------------------------------- Aggreate Functions ----------------------------------------//

	template <typename UDF_OP, typename STATE, typename TR, typename TA> void CreateAggregateFunction(string name) {
		AggregateFunction function = UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA>(name);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	void CreateAggregateFunction(string name) {
		AggregateFunction function = UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	void CreateAggregateFunction(string name, LogicalType ret_type, LogicalType input_typeA) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA>(name, ret_type, input_typeA);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	void CreateAggregateFunction(string name, LogicalType ret_type, LogicalType input_typeA, LogicalType input_typeB) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, ret_type, input_typeA, input_typeB);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	void CreateAggregateFunction(string name, vector<LogicalType> arguments, LogicalType return_type, aggregate_size_t state_size,
                                 aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
                                 aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
                                 bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr)
	{
		AggregateFunction function = UDFWrapper::CreateAggregateFunction(name, arguments, return_type, state_size, initialize,
																		 update, combine, finalize, simple_update, bind, destructor);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

private:
	unique_ptr<QueryResult> QueryParamsRecursive(string query, vector<Value> &values);

	template <typename T, typename... Args>
	unique_ptr<QueryResult> QueryParamsRecursive(string query, vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return QueryParamsRecursive(query, values, args...);
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/config.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {
class ClientContext;

enum class AccessMode : uint8_t { UNDEFINED = 0, AUTOMATIC = 1, READ_ONLY = 2, READ_WRITE = 3 };

// this is optional and only used in tests at the moment
struct DBConfig {
	friend class DuckDB;
	friend class StorageManager;

public:
	~DBConfig();

	//! Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)
	AccessMode access_mode = AccessMode::AUTOMATIC;
	// Checkpoint when WAL reaches this size
	idx_t checkpoint_wal_size = 1 << 20;
	//! Whether or not to use Direct IO, bypassing operating system buffers
	bool use_direct_io = false;
	//! The FileSystem to use, can be overwritten to allow for injecting custom file systems for testing purposes (e.g.
	//! RamFS or something similar)
	unique_ptr<FileSystem> file_system;
	//! The maximum memory used by the database system (in bytes). Default: Infinite
	idx_t maximum_memory = (idx_t)-1;
	//! Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
	bool use_temporary_directory = true;
	//! Directory to store temporary structures that do not fit in memory
	string temporary_directory;
	//! The collation type of the database
	string collation = string();
	//! The order type used when none is specified (default: ASC)
	OrderType default_order_type = OrderType::ASCENDING;
	//! Null ordering used when none is specified (default: NULLS FIRST)
	OrderByNullType default_null_order = OrderByNullType::NULLS_FIRST;
	//! enable COPY and related commands
	bool enable_copy = true;

public:
	static DBConfig &GetConfig(ClientContext &context);

private:
	// FIXME: don't set this as a user: used internally (only for now)
	bool checkpoint_only = false;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {
class DuckDB;

//! The Extension class is the base class used to define extensions
class Extension {
public:
	virtual void Load(DuckDB &db) = 0;
	virtual ~Extension() = default;
};

} // namespace duckdb


namespace duckdb {
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;
class TaskScheduler;

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class Connection;
class DuckDB {
public:
	DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DuckDB(const string &path, DBConfig *config = nullptr);

	~DuckDB();

	DBConfig config;

	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	unique_ptr<TaskScheduler> scheduler;
	unique_ptr<ConnectionManager> connection_manager;

public:
	template <class T> void LoadExtension() {
		T extension;
		extension.Load(*this);
	}

	FileSystem &GetFileSystem();

	static const char *SourceID();
	static const char *LibraryVersion();

private:
	void Configure(DBConfig &config);
};

} // namespace duckdb


//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//



#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t idx_t;

typedef enum DUCKDB_TYPE {
	DUCKDB_TYPE_INVALID = 0,
	// bool
	DUCKDB_TYPE_BOOLEAN,
	// int8_t
	DUCKDB_TYPE_TINYINT,
	// int16_t
	DUCKDB_TYPE_SMALLINT,
	// int32_t
	DUCKDB_TYPE_INTEGER,
	// int64_t
	DUCKDB_TYPE_BIGINT,
	// float
	DUCKDB_TYPE_FLOAT,
	// double
	DUCKDB_TYPE_DOUBLE,
	// duckdb_timestamp
	DUCKDB_TYPE_TIMESTAMP,
	// duckdb_date
	DUCKDB_TYPE_DATE,
	// duckdb_time
	DUCKDB_TYPE_TIME,
	// duckdb_interval
	DUCKDB_TYPE_INTERVAL,
	// duckdb_hugeint
	DUCKDB_TYPE_HUGEINT,
	// const char*
	DUCKDB_TYPE_VARCHAR
} duckdb_type;

typedef struct {
	int32_t year;
	int8_t month;
	int8_t day;
} duckdb_date;

typedef struct {
	int8_t hour;
	int8_t min;
	int8_t sec;
	int16_t msec;
} duckdb_time;

typedef struct {
	duckdb_date date;
	duckdb_time time;
} duckdb_timestamp;

typedef struct {
	int32_t months;
	int32_t days;
	int64_t msecs;
} duckdb_interval;

typedef struct {
	uint64_t lower;
	int64_t upper;
} duckdb_hugeint;

typedef struct {
	void *data;
	bool *nullmask;
	duckdb_type type;
	char *name;
} duckdb_column;

typedef struct {
	idx_t column_count;
	idx_t row_count;
	duckdb_column *columns;
	char *error_message;
} duckdb_result;

// typedef struct {
// 	void *data;
// 	bool *nullmask;
// } duckdb_column_data;

// typedef struct {
// 	int column_count;
// 	int count;
// 	duckdb_column_data *columns;
// } duckdb_chunk;

typedef void *duckdb_database;
typedef void *duckdb_connection;
typedef void *duckdb_prepared_statement;

typedef enum { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;

//! Opens a database file at the given path (nullptr for in-memory). Returns DuckDBSuccess on success, or DuckDBError on
//! failure. [OUT: database]
duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
//! Closes the database.
void duckdb_close(duckdb_database *database);

//! Creates a connection to the specified database. [OUT: connection]
duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
//! Closes the specified connection handle
void duckdb_disconnect(duckdb_connection *connection);

//! Executes the specified SQL query in the specified connection handle. [OUT: result descriptor]
duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
//! Destroys the specified result
void duckdb_destroy_result(duckdb_result *result);

//! Returns the column name of the specified column. The result does not need to be freed;
//! the column names will automatically be destroyed when the result is destroyed.
const char *duckdb_column_name(duckdb_result *result, idx_t col);

// SAFE fetch functions
// These functions will perform conversions if necessary. On failure (e.g. if conversion cannot be performed) a special
// value is returned.

//! Converts the specified value to a bool. Returns false on failure or NULL.
bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int8_t. Returns 0 on failure or NULL.
int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int16_t. Returns 0 on failure or NULL.
int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int64_t. Returns 0 on failure or NULL.
int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int64_t. Returns 0 on failure or NULL.
int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to a float. Returns 0.0 on failure or NULL.
float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to a double. Returns 0.0 on failure or NULL.
double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to a string. Returns nullptr on failure or NULL. The result must be freed with free.
char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row);

// Prepared Statements

//! prepares the specified SQL query in the specified connection handle. [OUT: prepared statement descriptor]
duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                            duckdb_prepared_statement *out_prepared_statement);

duckdb_state duckdb_nparams(duckdb_prepared_statement prepared_statement, idx_t *nparams_out);

//! binds parameters to prepared statement
duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);
duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val);
duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val);
duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);
duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val);
duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val);
duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);

//! Executes the prepared statements with currently bound parameters
duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result);

//! Destroys the specified prepared statement descriptor
void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);

#ifdef __cplusplus
}
#endif
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/date.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

//! The Date class is a static class that holds helper functions for the Date
//! type.
class Date {
public:
	static string_t MonthNames[12];
	static string_t MonthNamesAbbreviated[12];
	static string_t DayNames[7];
	static string_t DayNamesAbbreviated[7];

public:
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	static date_t FromString(string str, bool strict = false);
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	static date_t FromCString(const char *str, bool strict = false);
	//! Convert a date object to a string in the format "YYYY-MM-DD"
	static string ToString(date_t date);
	//! Try to convert text in a buffer to a date; returns true if parsing was successful
	static bool TryConvertDate(const char *buf, idx_t &pos, date_t &result, bool strict = false);

	//! Create a string "YYYY-MM-DD" from a specified (year, month, day)
	//! combination
	static string Format(int32_t year, int32_t month, int32_t day);

	//! Extract the year, month and day from a given date object
	static void Convert(date_t date, int32_t &out_year, int32_t &out_month, int32_t &out_day);
	//! Create a Date object from a specified (year, month, day) combination
	static date_t FromDate(int32_t year, int32_t month, int32_t day);

	//! Returns true if (year) is a leap year, and false otherwise
	static bool IsLeapYear(int32_t year);

	//! Returns true if the specified (year, month, day) combination is a valid
	//! date
	static bool IsValidDay(int32_t year, int32_t month, int32_t day);

	//! Extract the epoch from the date (seconds since 1970-01-01)
	static int64_t Epoch(date_t date);
	//! Convert the epoch (seconds since 1970-01-01) to a date_t
	static date_t EpochToDate(int64_t epoch);
	//! Extract year of a date entry
	static int32_t ExtractYear(date_t date);
	//! Extract month of a date entry
	static int32_t ExtractMonth(date_t date);
	//! Extract day of a date entry
	static int32_t ExtractDay(date_t date);
	//! Extract the day of the week (1-7)
	static int32_t ExtractISODayOfTheWeek(date_t date);
	//! Extract the day of the year
	static int32_t ExtractDayOfTheYear(date_t date);
	//! Extract the ISO week number
	//! ISO weeks start on Monday and the first week of a year
	//! contains January 4 of that year.
	//! In the ISO week-numbering system, it is possible for early-January dates
	//! to be part of the 52nd or 53rd week of the previous year.
	static int32_t ExtractISOWeekNumber(date_t date);
	//! Extract the week number as Python handles it.
	//! Either Monday or Sunday is the first day of the week,
	//! and any date before the first Monday/Sunday returns week 0
	//! This is a bit more consistent because week numbers in a year are always incrementing
	static int32_t ExtractWeekNumberRegular(date_t date, bool monday_first = true);
	//! Returns the date of the monday of the current week.
	static date_t GetMondayOfCurrentWeek(date_t date);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#ifndef ARROW_FLAG_DICTIONARY_ORDERED

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
	// Array type description
	const char *format;
	const char *name;
	const char *metadata;
	int64_t flags;
	int64_t n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;

	// Release callback
	void (*release)(struct ArrowSchema *);
	// Opaque producer-specific data
	void *private_data;
};

struct ArrowArray {
	// Array data description
	int64_t length;
	int64_t null_count;
	int64_t offset;
	int64_t n_buffers;
	int64_t n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;

	// Release callback
	void (*release)(struct ArrowArray *);
	// Opaque producer-specific data
	void *private_data;
};

// EXPERIMENTAL
struct ArrowArrayStream {
	// Callback to get the stream type
	// (will be the same for all arrays in the stream).
	// Return value: 0 if successful, an `errno`-compatible error code otherwise.
	int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
	// Callback to get the next array
	// (if no error and the array is released, the stream has ended)
	// Return value: 0 if successful, an `errno`-compatible error code otherwise.
	int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out);

	// Callback to get optional detailed error information.
	// This must only be called if the last stream operation failed
	// with a non-0 return code.  The returned pointer is only valid until
	// the next operation on this stream (including release).
	// If unavailable, NULL is returned.
	const char *(*get_last_error)(struct ArrowArrayStream *);

	// Release callback: release the stream's own resources.
	// Note that arrays returned by `get_next` must be individually released.
	void (*release)(struct ArrowArrayStream *);
	// Opaque producer-specific data
	void *private_data;
};

#ifdef __cplusplus
}
#endif

#endif
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/decimal.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! The Decimal class is a static class that holds helper functions for the Decimal type
class Decimal {
public:
	static constexpr uint8_t MAX_WIDTH_INT16 = 4;
	static constexpr uint8_t MAX_WIDTH_INT32 = 9;
	static constexpr uint8_t MAX_WIDTH_INT64 = 18;
	static constexpr uint8_t MAX_WIDTH_INT128 = 38;
	static constexpr uint8_t MAX_WIDTH_DECIMAL = MAX_WIDTH_INT128;

public:
	static string ToString(int16_t value, uint8_t scale);
	static string ToString(int32_t value, uint8_t scale);
	static string ToString(int64_t value, uint8_t scale);
	static string ToString(hugeint_t value, uint8_t scale);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hugeint.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/limits.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

template <class T> struct NumericLimits {
	static T Minimum();
	static T Maximum();
};

template <> struct NumericLimits<int8_t> {
	static int8_t Minimum();
	static int8_t Maximum();
};
template <> struct NumericLimits<int16_t> {
	static int16_t Minimum();
	static int16_t Maximum();
};
template <> struct NumericLimits<int32_t> {
	static int32_t Minimum();
	static int32_t Maximum();
};
template <> struct NumericLimits<int64_t> {
	static int64_t Minimum();
	static int64_t Maximum();
};
template <> struct NumericLimits<hugeint_t> {
	static hugeint_t Minimum();
	static hugeint_t Maximum();
};
template <> struct NumericLimits<uint16_t> {
	static uint16_t Minimum();
	static uint16_t Maximum();
};
template <> struct NumericLimits<uint32_t> {
	static uint32_t Minimum();
	static uint32_t Maximum();
};
template <> struct NumericLimits<uint64_t> {
	static uint64_t Minimum();
	static uint64_t Maximum();
};
template <> struct NumericLimits<float> {
	static float Minimum();
	static float Maximum();
};
template <> struct NumericLimits<double> {
	static double Minimum();
	static double Maximum();
};

//! Returns the minimal type that guarantees an integer value from not
//! overflowing
PhysicalType MinimalType(int64_t value);

} // namespace duckdb


namespace duckdb {

//! The Hugeint class contains static operations for the INT128 type
class Hugeint {
public:
	//! Convert a string to a hugeint object
	static bool FromString(string str, hugeint_t &result);
	//! Convert a string to a hugeint object
	static bool FromCString(const char *str, idx_t len, hugeint_t &result);
	//! Convert a hugeint object to a string
	static string ToString(hugeint_t input);

	static hugeint_t FromString(string str) {
		hugeint_t result;
		FromString(str, result);
		return result;
	}

	template <class T> static bool TryCast(hugeint_t input, T &result);

	template <class T> static T Cast(hugeint_t input) {
		T value;
		TryCast(input, value);
		return value;
	}

	template <class T> static hugeint_t Convert(T value);

	static void NegateInPlace(hugeint_t &input) {
		input.lower = NumericLimits<uint64_t>::Maximum() - input.lower + 1;
		input.upper = -1 - input.upper + (input.lower == 0);
	}
	static hugeint_t Negate(hugeint_t input) {
		NegateInPlace(input);
		return input;
	}

	static bool TryMultiply(hugeint_t lhs, hugeint_t rhs, hugeint_t &result);

	static hugeint_t Add(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Subtract(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Multiply(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Divide(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Modulo(hugeint_t lhs, hugeint_t rhs);

	// DivMod -> returns the result of the division (lhs / rhs), and fills up the remainder
	static hugeint_t DivMod(hugeint_t lhs, hugeint_t rhs, hugeint_t &remainder);
	// DivMod but lhs MUST be positive, and rhs is a uint64_t
	static hugeint_t DivModPositive(hugeint_t lhs, uint64_t rhs, uint64_t &remainder);

	static bool AddInPlace(hugeint_t &lhs, hugeint_t rhs);
	static bool SubtractInPlace(hugeint_t &lhs, hugeint_t rhs);

	// comparison operators
	// note that everywhere here we intentionally use bitwise ops
	// this is because they seem to be consistently much faster (benchmarked on a Macbook Pro)
	static bool Equals(hugeint_t lhs, hugeint_t rhs) {
		int lower_equals = lhs.lower == rhs.lower;
		int upper_equals = lhs.upper == rhs.upper;
		return lower_equals & upper_equals;
	}
	static bool NotEquals(hugeint_t lhs, hugeint_t rhs) {
		int lower_not_equals = lhs.lower != rhs.lower;
		int upper_not_equals = lhs.upper != rhs.upper;
		return lower_not_equals | upper_not_equals;
	}
	static bool GreaterThan(hugeint_t lhs, hugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger = lhs.lower > rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger);
	}
	static bool GreaterThanEquals(hugeint_t lhs, hugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger_equals = lhs.lower >= rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger_equals);
	}
	static bool LessThan(hugeint_t lhs, hugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller = lhs.lower < rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller);
	}
	static bool LessThanEquals(hugeint_t lhs, hugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller_equals = lhs.lower <= rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller_equals);
	}
	static hugeint_t PowersOfTen[40];
};

template <> bool Hugeint::TryCast(hugeint_t input, int8_t &result);
template <> bool Hugeint::TryCast(hugeint_t input, int16_t &result);
template <> bool Hugeint::TryCast(hugeint_t input, int32_t &result);
template <> bool Hugeint::TryCast(hugeint_t input, int64_t &result);
template <> bool Hugeint::TryCast(hugeint_t input, hugeint_t &result);
template <> bool Hugeint::TryCast(hugeint_t input, float &result);
template <> bool Hugeint::TryCast(hugeint_t input, double &result);

template <> hugeint_t Hugeint::Convert(int8_t value);
template <> hugeint_t Hugeint::Convert(int16_t value);
template <> hugeint_t Hugeint::Convert(int32_t value);
template <> hugeint_t Hugeint::Convert(int64_t value);
template <> hugeint_t Hugeint::Convert(float value);
template <> hugeint_t Hugeint::Convert(double value);
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/interval.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! The Interval class is a static class that holds helper functions for the Interval
//! type.
class Interval {
public:
	static constexpr const int32_t MONTHS_PER_MILLENIUM = 12000;
	static constexpr const int32_t MONTHS_PER_CENTURY = 1200;
	static constexpr const int32_t MONTHS_PER_DECADE = 120;
	static constexpr const int32_t MONTHS_PER_YEAR = 12;
	static constexpr const int32_t MONTHS_PER_QUARTER = 3;
	static constexpr const int32_t DAYS_PER_WEEK = 7;
	static constexpr const int64_t DAYS_PER_MONTH = 30; // only used for comparison purposes, in which case a month counts as 30 days
	static constexpr const int64_t MSECS_PER_SEC = 1000;
	static constexpr const int64_t MSECS_PER_MINUTE = MSECS_PER_SEC * 60;
	static constexpr const int64_t MSECS_PER_HOUR = MSECS_PER_MINUTE * 60;
	static constexpr const int64_t MSECS_PER_DAY = MSECS_PER_HOUR * 24;
	static constexpr const int64_t MSECS_PER_MONTH = MSECS_PER_DAY * DAYS_PER_MONTH;
	static constexpr const int32_t SECS_PER_MINUTE = 60;
	static constexpr const int32_t MINS_PER_HOUR = 60;
	static constexpr const int32_t HOURS_PER_DAY = 24;

public:
	//! Convert a string to an interval object
	static bool FromString(string str, interval_t &result);
	//! Convert a string to an interval object
	static bool FromCString(const char *str, idx_t len, interval_t &result);
	//! Convert an interval object to a string
	static string ToString(interval_t date);

	//! Returns the difference between two timestamps
	static interval_t GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2);

	//! Comparison operators
	static bool Equals(interval_t left, interval_t right);
	static bool GreaterThan(interval_t left, interval_t right);
	static bool GreaterThanEquals(interval_t left, interval_t right);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/timestamp.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

struct timestamp_struct {
	int32_t year;
	int8_t month;
	int8_t day;
	int8_t hour;
	int8_t min;
	int8_t sec;
	int16_t msec;
};
//! The Timestamp class is a static class that holds helper functions for the Timestamp
//! type.
class Timestamp {
public:
	//! Convert a string in the format "YYYY-MM-DD hh:mm:ss" to a timestamp object
	static timestamp_t FromString(string str);
	static timestamp_t FromCString(const char *str, idx_t len);
	//! Convert a date object to a string in the format "YYYY-MM-DD hh:mm:ss"
	static string ToString(timestamp_t timestamp);

	static date_t GetDate(timestamp_t timestamp);

	static dtime_t GetTime(timestamp_t timestamp);
	//! Create a Timestamp object from a specified (date, time) combination
	static timestamp_t FromDatetime(date_t date, dtime_t time);
	//! Extract the date and time from a given timestamp object
	static void Convert(timestamp_t date, date_t &out_date, dtime_t &out_time);
	//! Returns current timestamp
	static timestamp_t GetCurrentTimestamp();

	// Unix epoch: milliseconds since 1970
	static int64_t GetEpoch(timestamp_t timestamp);
	// Seconds including fractional part multiplied by 1000
	static int64_t GetMilliseconds(timestamp_t timestamp);
	static int64_t GetSeconds(timestamp_t timestamp);
	static int64_t GetMinutes(timestamp_t timestamp);
	static int64_t GetHours(timestamp_t timestamp);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/time.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! The Time class is a static class that holds helper functions for the Time
//! type.
class Time {
public:
	//! Convert a string in the format "hh:mm:ss" to a time object
	static dtime_t FromString(string str, bool strict = false);
	static dtime_t FromCString(const char *buf, bool strict = false);
	static bool TryConvertTime(const char *buf, idx_t &pos, dtime_t &result, bool strict = false);

	//! Convert a time object to a string in the format "hh:mm:ss"
	static string ToString(dtime_t time);

	static string Format(int32_t hour, int32_t minute, int32_t second, int32_t milisecond = 0);

	static dtime_t FromTime(int32_t hour, int32_t minute, int32_t second, int32_t milisecond = 0);

	static bool IsValidTime(int32_t hour, int32_t minute, int32_t second, int32_t milisecond = 0);
	//! Extract the time from a given timestamp object
	static void Convert(dtime_t time, int32_t &out_hour, int32_t &out_min, int32_t &out_sec, int32_t &out_msec);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_serializer.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

#define SERIALIZER_DEFAULT_SIZE 1024

struct BinaryData {
	unique_ptr<data_t[]> data;
	idx_t size;
};

class BufferedSerializer : public Serializer {
public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	BufferedSerializer(idx_t maximum_size = SERIALIZER_DEFAULT_SIZE);
	//! Serializes to a provided (owned) data pointer
	BufferedSerializer(unique_ptr<data_t[]> data, idx_t size);
	BufferedSerializer(data_ptr_t data, idx_t size);

	idx_t maximum_size;
	data_ptr_t data;

	BinaryData blob;

public:
	void WriteData(const_data_ptr_t buffer, uint64_t write_size) override;

	//! Retrieves the data after the writing has been completed
	BinaryData GetData() {
		return std::move(blob);
	}

	void Reset() {
		blob.size = 0;
	}
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/appender.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

class ClientContext;
class DuckDB;
class TableCatalogEntry;
class Connection;

//! The Appender class can be used to append elements to a table.
class Appender {
	//! A reference to a database connection that created this appender
	Connection &con;
	//! The table description (including column names)
	unique_ptr<TableDescription> description;
	//! Internal chunk used for appends
	DataChunk chunk;
	//! The current column to append to
	idx_t column = 0;
	//! Message explaining why the Appender is invalidated (if any)
	string invalidated_msg;

public:
	Appender(Connection &con, string schema_name, string table_name);
	Appender(Connection &con, string table_name);
	~Appender();

	//! Begins a new row append, after calling this the other AppendX() functions
	//! should be called the correct amount of times. After that,
	//! EndRow() should be called.
	void BeginRow();
	//! Finishes appending the current row.
	void EndRow();

	// Append functions
	template <class T> void Append(T value) {
		throw Exception("Undefined type for Appender::Append!");
	}

	void Append(const char *value, uint32_t length);

	// prepared statements
	template <typename... Args> void AppendRow(Args... args) {
		BeginRow();
		AppendRowRecursive(args...);
	}

	//! Commit the changes made by the appender.
	void Flush();
	//! Flush the changes made by the appender and close it. The appender cannot be used after this point
	void Close();

	//! Obtain a reference to the internal vector that is used to append to the table
	DataChunk &GetAppendChunk() {
		return chunk;
	}

	idx_t CurrentColumn() {
		return column;
	}

	void Invalidate(string msg, bool close = true);

private:
	//! Invalidate the appender with a specific message and throw an exception with the same message
	void InvalidateException(string msg);

	template <class T> void AppendValueInternal(T value);
	template <class SRC, class DST> void AppendValueInternal(Vector &vector, SRC input);

	void CheckInvalidated();

	void AppendRowRecursive() {
		EndRow();
	}

	template <typename T, typename... Args> void AppendRowRecursive(T value, Args... args) {
		Append<T>(value);
		AppendRowRecursive(args...);
	}

	void AppendValue(Value value);
};

template <> void Appender::Append(bool value);
template <> void Appender::Append(int8_t value);
template <> void Appender::Append(int16_t value);
template <> void Appender::Append(int32_t value);
template <> void Appender::Append(int64_t value);
template <> void Appender::Append(float value);
template <> void Appender::Append(double value);
template <> void Appender::Append(const char *value);
template <> void Appender::Append(Value value);
template <> void Appender::Append(std::nullptr_t value);

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/schema_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/catalog_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class CatalogType : uint8_t {
	INVALID = 0,
	TABLE_ENTRY = 1,
	SCHEMA_ENTRY = 2,
	TABLE_FUNCTION_ENTRY = 3,
	SCALAR_FUNCTION_ENTRY = 4,
	AGGREGATE_FUNCTION_ENTRY = 5,
	COPY_FUNCTION = 6,
	VIEW_ENTRY = 7,
	INDEX_ENTRY = 8,
	PREPARED_STATEMENT = 9,
	SEQUENCE_ENTRY = 10,
	COLLATION_ENTRY = 11,

	UPDATED_ENTRY = 50,
	DELETED_ENTRY = 51,
};

string CatalogTypeToString(CatalogType type);

} // namespace duckdb


#include <memory>

namespace duckdb {
struct AlterInfo;
class Catalog;
class CatalogSet;
class ClientContext;

//! Abstract base class of an entry in the catalog
class CatalogEntry {
public:
	CatalogEntry(CatalogType type, Catalog *catalog, string name)
	    : type(type), catalog(catalog), set(nullptr), name(name), deleted(false), temporary(false), parent(nullptr) {
	}
	virtual ~CatalogEntry();

	virtual unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) {
		throw CatalogException("Unsupported alter type for catalog entry!");
	}

	virtual unique_ptr<CatalogEntry> Copy(ClientContext &context) {
		throw CatalogException("Unsupported copy type for catalog entry!");
	}
	//! Sets the CatalogEntry as the new root entry (i.e. the newest entry) - this is called on a rollback to an
	//! AlterEntry
	virtual void SetAsRoot() {
	}
	//! Convert the catalog entry to a SQL string that can be used to re-construct the catalog entry
	virtual string ToSQL() {
		throw CatalogException("Unsupported catalog type for ToSQL()");
	}

	//! The type of this catalog entry
	CatalogType type;
	//! Reference to the catalog this entry belongs to
	Catalog *catalog;
	//! Reference to the catalog set this entry is stored in
	CatalogSet *set;
	//! The name of the entry
	string name;
	//! Whether or not the object is deleted
	bool deleted;
	//! Whether or not the object is temporary and should not be added to the WAL
	bool temporary;
	//! Timestamp at which the catalog entry was created
	transaction_t timestamp;
	//! Child entry
	unique_ptr<CatalogEntry> child;
	//! Parent entry (the node that owns this node)
	CatalogEntry *parent;
};
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_map.hpp
//
//
//===----------------------------------------------------------------------===//



#include <unordered_map>

namespace duckdb {
using std::unordered_map;
}


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/mutex.hpp
//
//
//===----------------------------------------------------------------------===//



#include <mutex>

namespace duckdb {
using std::mutex;
}


#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class Transaction;

typedef unordered_map<CatalogSet *, std::unique_lock<std::mutex>> set_lock_map_t;

//! The Catalog Set stores (key, value) map of a set of AbstractCatalogEntries
class CatalogSet {
	friend class DependencyManager;

public:
	CatalogSet(Catalog &catalog);

	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	bool CreateEntry(Transaction &transaction, const string &name, unique_ptr<CatalogEntry> value,
	                 unordered_set<CatalogEntry *> &dependencies);

	bool AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info);

	bool DropEntry(Transaction &transaction, const string &name, bool cascade);
	//! Returns the entry with the specified name
	CatalogEntry *GetEntry(Transaction &transaction, const string &name);
	//! Returns the root entry with the specified name regardless of transaction (or nullptr if there are none)
	CatalogEntry *GetRootEntry(const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry *entry);

	//! Scan the catalog set, invoking the callback method for every entry
	template <class T> void Scan(Transaction &transaction, T &&callback) {
		// lock the catalog set
		std::lock_guard<std::mutex> lock(catalog_lock);
		for (auto &kv : data) {
			auto entry = kv.second.get();
			entry = GetEntryForTransaction(transaction, entry);
			if (!entry->deleted) {
				callback(entry);
			}
		}
	}

	static bool HasConflict(Transaction &transaction, CatalogEntry &current);

private:
	//! Drops an entry from the catalog set; must hold the catalog_lock to
	//! safely call this
	void DropEntryInternal(Transaction &transaction, CatalogEntry &entry, bool cascade, set_lock_map_t &lock_set);
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(Transaction &transaction, CatalogEntry *current);

	Catalog &catalog;
	//! The catalog lock is used to make changes to the data
	mutex catalog_lock;
	//! The set of entries present in the CatalogSet.
	unordered_map<string, unique_ptr<CatalogEntry>> data;
};

} // namespace duckdb


namespace duckdb {
class ClientContext;

class StandardEntry;
class TableCatalogEntry;
class TableFunctionCatalogEntry;
class SequenceCatalogEntry;
class Serializer;
class Deserializer;

enum class OnCreateConflict : uint8_t;

struct AlterTableInfo;
struct CreateIndexInfo;
struct CreateFunctionInfo;
struct CreateCollationInfo;
struct CreateViewInfo;
struct BoundCreateTableInfo;
struct CreateSequenceInfo;
struct CreateSchemaInfo;
struct CreateTableFunctionInfo;
struct CreateCopyFunctionInfo;

struct DropInfo;

//! A schema in the catalog
class SchemaCatalogEntry : public CatalogEntry {
public:
	SchemaCatalogEntry(Catalog *catalog, string name);

	//! The catalog set holding the tables
	CatalogSet tables;
	//! The catalog set holding the indexes
	CatalogSet indexes;
	//! The catalog set holding the table functions
	CatalogSet table_functions;
	//! The catalog set holding the copy functions
	CatalogSet copy_functions;
	//! The catalog set holding the scalar and aggregate functions
	CatalogSet functions;
	//! The catalog set holding the sequences
	CatalogSet sequences;
	//! The catalog set holding the collations
	CatalogSet collations;

public:
	//! Creates a table with the given name in the schema
	CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Creates a view with the given name in the schema
	CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a sequence with the given name in the schema
	CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Creates an index with the given name in the schema
	CatalogEntry *CreateIndex(ClientContext &context, CreateIndexInfo *info);
	//! Create a table function within the given schema
	CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function within the given schema
	CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a scalar or aggregate function within the given schema
	CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Create a collation within the given schema
	CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Drops an entry from the schema
	void DropEntry(ClientContext &context, DropInfo *info);

	//! Alters a table
	void AlterTable(ClientContext &context, AlterTableInfo *info);

	//! Gets a catalog entry from the given catalog set matching the given name
	CatalogEntry *GetEntry(ClientContext &context, CatalogType type, const string &name, bool if_exists);

	//! Serialize the meta information of the SchemaCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateSchemaInfo
	static unique_ptr<CreateSchemaInfo> Deserialize(Deserializer &source);

	string ToSQL() override;

private:
	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict);
	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict,
	                       unordered_set<CatalogEntry *> dependencies);

	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace duckdb



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/executor.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_sink.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
struct CreateSchemaInfo;
struct DropInfo;
struct BoundCreateTableInfo;
struct AlterTableInfo;
struct CreateTableFunctionInfo;
struct CreateCopyFunctionInfo;
struct CreateFunctionInfo;
struct CreateViewInfo;
struct CreateSequenceInfo;
struct CreateCollationInfo;

class ClientContext;
class Transaction;

class AggregateFunctionCatalogEntry;
class CollateCatalogEntry;
class SchemaCatalogEntry;
class TableCatalogEntry;
class SequenceCatalogEntry;
class TableFunctionCatalogEntry;
class CopyFunctionCatalogEntry;
class StorageManager;
class CatalogSet;
class DependencyManager;

//! The Catalog object represents the catalog of the database.
class Catalog {
public:
	Catalog(StorageManager &storage);
	~Catalog();

	//! Reference to the storage manager
	StorageManager &storage;
	//! The catalog set holding the schemas
	unique_ptr<CatalogSet> schemas;
	//! The DependencyManager manages dependencies between different catalog objects
	unique_ptr<DependencyManager> dependency_manager;
	//! Write lock for the catalog
	mutex write_lock;

public:
	//! Get the ClientContext from the Catalog
	static Catalog &GetCatalog(ClientContext &context);

	//! Creates a schema in the catalog.
	CatalogEntry *CreateSchema(ClientContext &context, CreateSchemaInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Creates a collation in the catalog
	CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Drops an entry from the catalog
	void DropEntry(ClientContext &context, DropInfo *info);

	//! Returns the schema object with the specified name, or throws an exception if it does not exist
	SchemaCatalogEntry *GetSchema(ClientContext &context, const string &name = DEFAULT_SCHEMA);
	//! Gets the "schema.name" entry of the specified type, if if_exists=true returns nullptr if entry does not exist,
	//! otherwise an exception is thrown
	CatalogEntry *GetEntry(ClientContext &context, CatalogType type, string schema, const string &name,
	                       bool if_exists = false);
	template <class T>
	T *GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists = false);

	//! Alter an existing table in the catalog.
	void AlterTable(ClientContext &context, AlterTableInfo *info);

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to DEFAULT_SCHEMA
	static void ParseRangeVar(string input, string &schema, string &name);

private:
	void DropSchema(ClientContext &context, DropInfo *info);
};

template <>
TableCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists);
template <>
SequenceCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists);
template <>
TableFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                             bool if_exists);
template <>
CopyFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                            bool if_exists);
template <>
AggregateFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                                 bool if_exists);
template <>
CollateCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists);

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/physical_operator_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Physical Operator Types
//===--------------------------------------------------------------------===//
enum class PhysicalOperatorType : uint8_t {
	INVALID,
	LEAF,
	ORDER_BY,
	LIMIT,
	TOP_N,
	AGGREGATE,
	WINDOW,
	UNNEST,
	DISTINCT,
	SIMPLE_AGGREGATE,
	HASH_GROUP_BY,
	SORT_GROUP_BY,
	FILTER,
	PROJECTION,
	COPY_FROM_FILE,
	COPY_TO_FILE,
	TABLE_FUNCTION,
	// -----------------------------
	// Scans
	// -----------------------------
	DUMMY_SCAN,
	SEQ_SCAN,
	INDEX_SCAN,
	CHUNK_SCAN,
	DELIM_SCAN,
	EXTERNAL_FILE_SCAN,
	QUERY_DERIVED_SCAN,
	EXPRESSION_SCAN,
	// -----------------------------
	// Joins
	// -----------------------------
	BLOCKWISE_NL_JOIN,
	NESTED_LOOP_JOIN,
	HASH_JOIN,
	CROSS_PRODUCT,
	PIECEWISE_MERGE_JOIN,
	DELIM_JOIN,

	// -----------------------------
	// SetOps
	// -----------------------------
	UNION,
	RECURSIVE_CTE,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	INSERT_SELECT,
	DELETE,
	UPDATE,
	EXPORT_EXTERNAL_FILE,

	// -----------------------------
	// Schema
	// -----------------------------
	CREATE,
	CREATE_INDEX,
	ALTER,
	CREATE_SEQUENCE,
	CREATE_VIEW,
	CREATE_SCHEMA,
	DROP,
	PRAGMA,
	TRANSACTION,

	// -----------------------------
	// Helpers
	// -----------------------------
	EXPLAIN,
	EMPTY_RESULT,
	EXECUTE,
	PREPARE,
	VACUUM,
	EXPORT
};

string PhysicalOperatorToString(PhysicalOperatorType type);

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
//!  The Expression class represents a bound Expression with a return type
class Expression : public BaseExpression {
public:
	Expression(ExpressionType type, ExpressionClass expression_class, LogicalType return_type);

	//! The return type of the expression
	LogicalType return_type;

public:
	bool IsAggregate() const override;
	bool IsWindow() const override;
	bool HasSubquery() const override;
	bool IsScalar() const override;
	bool HasParameter() const override;
	virtual bool IsFoldable() const;

	hash_t Hash() const override;

	virtual bool Equals(const BaseExpression *other) const override {
		if (!BaseExpression::Equals(other)) {
			return false;
		}
		return return_type == ((Expression *) other)->return_type;
	}

	static bool Equals(Expression *left, Expression *right) {
		return BaseExpression::Equals((BaseExpression *)left, (BaseExpression *)right);
	}
	//! Create a copy of this expression
	virtual unique_ptr<Expression> Copy() = 0;

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(Expression &other) {
		type = other.type;
		expression_class = other.expression_class;
		alias = other.alias;
		return_type = other.return_type;
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {
class ClientContext;
class ThreadContext;
class TaskContext;

class ExecutionContext {
public:
	ExecutionContext(ClientContext &client_, ThreadContext &thread_, TaskContext &task_)
	    : client(client_), thread(thread_), task(task_) {
	}

	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext &client;
	//! The thread-local context for this execution
	ThreadContext &thread;
	//! The task context for this execution
	TaskContext &task;
};

} // namespace duckdb


#include <functional>

namespace duckdb {
class ExpressionExecutor;
class PhysicalOperator;
class OperatorTaskInfo;

//! The current state/context of the operator. The PhysicalOperatorState is
//! updated using the GetChunk function, and allows the caller to repeatedly
//! call the GetChunk function and get new batches of data everytime until the
//! data source is exhausted.
class PhysicalOperatorState {
public:
	PhysicalOperatorState(PhysicalOperator *child);
	virtual ~PhysicalOperatorState() = default;

	//! Flag indicating whether or not the operator is finished [note: not all
	//! operators use this flag]
	bool finished;
	//! DataChunk that stores data from the child of this operator
	DataChunk child_chunk;
	//! State of the child of this operator
	unique_ptr<PhysicalOperatorState> child_state;
};

//! PhysicalOperator is the base class of the physical operators present in the
//! execution plan
/*!
    The execution model is a pull-based execution model. GetChunk is called on
   the root node, which causes the root node to be executed, and presumably call
   GetChunk again on its child nodes. Every node in the operator chain has a
   state that is updated as GetChunk is called: PhysicalOperatorState (different
   operators subclass this state and add different properties).
*/
class PhysicalOperator {
public:
	PhysicalOperator(PhysicalOperatorType type, vector<LogicalType> types) : type(type), types(types) {
	}
	virtual ~PhysicalOperator() {
	}

	//! The physical operator type
	PhysicalOperatorType type;
	//! The set of children of the operator
	vector<unique_ptr<PhysicalOperator>> children;
	//! The types returned by this physical operator
	vector<LogicalType> types;

public:
	string ToString(idx_t depth = 0) const;
	void Print();

	//! Return a vector of the types that will be returned by this operator
	vector<LogicalType> &GetTypes() {
		return types;
	}
	//! Initialize a given chunk to the types that will be returned by this
	//! operator, this will prepare chunk for a call to GetChunk. This method
	//! only has to be called once for any amount of calls to GetChunk.
	virtual void InitializeChunk(DataChunk &chunk) {
		auto &types = GetTypes();
		chunk.Initialize(types);
	}
	//! Retrieves a chunk from this operator and stores it in the chunk
	//! variable.
	virtual void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) = 0;

	void GetChunk(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state);

	//! Create a new empty instance of the operator state
	virtual unique_ptr<PhysicalOperatorState> GetOperatorState() {
		return make_unique<PhysicalOperatorState>(children.size() == 0 ? nullptr : children[0].get());
	}

	virtual string ExtraRenderInformation() const {
		return "";
	}

	virtual bool IsSink() const {
		return false;
	}

	//! Provides an interface for parallel scans of this operator. For every OperatorTaskInfo returned, one task is
	//! created. The OperatorTaskInfo can be accessed as part of the TaskContext during execution.
	virtual void ParallelScanInfo(ClientContext &context, std::function<void(unique_ptr<OperatorTaskInfo>)> callback);
};

} // namespace duckdb


namespace duckdb {

class GlobalOperatorState {
public:
	virtual ~GlobalOperatorState() {
	}
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() {
	}
};

class PhysicalSink : public PhysicalOperator {
public:
	PhysicalSink(PhysicalOperatorType type, vector<LogicalType> types) : PhysicalOperator(type, move(types)) {
	}

	unique_ptr<GlobalOperatorState> sink_state;

public:
	//! The sink method is called constantly with new input, as long as new input is available. Note that this method
	//! CAN be called in parallel, proper locking is needed when accessing data inside the GlobalOperatorState.
	virtual void Sink(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate,
	                  DataChunk &input) = 0;
	// The combine is called when a single thread has completed execution of its part of the pipeline, it is the final
	// time that a specific LocalSinkState is accessible. This method can be called in parallel while other Sink() or
	// Combine() calls are active on the same GlobalOperatorState.
	virtual void Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	}
	//! The finalize is called when ALL threads are finished execution. It is called only once per pipeline, and is
	//! entirely single threaded.
	virtual void Finalize(ClientContext &context, unique_ptr<GlobalOperatorState> gstate) {
		this->sink_state = move(gstate);
	}

	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) {
		return make_unique<LocalSinkState>();
	}
	virtual unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) {
		return make_unique<GlobalOperatorState>();
	}

	bool IsSink() const override {
		return true;
	}

	void Schedule(ClientContext &context);
};

} // namespace duckdb



#include <atomic>

namespace duckdb {
class Executor;
class TaskContext;

//! The Pipeline class represents an execution pipeline
class Pipeline {
	friend class Executor;

public:
	Pipeline(Executor &execution_context);

	Executor &executor;

public:
	//! Execute a task within the pipeline on a single thread
	void Execute(TaskContext &task);

	void AddDependency(Pipeline *pipeline);
	void CompleteDependency();
	bool HasDependencies() {
		return dependencies.size() != 0;
	}

	void Schedule();

	//! Finish a single task of this pipeline
	void FinishTask();
	//! Finish executing this pipeline
	void Finish();

	string ToString() const;
	void Print() const;

private:
	//! The child from which to pull chunks
	PhysicalOperator *child;
	//! The global sink state
	unique_ptr<GlobalOperatorState> sink_state;
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	PhysicalSink *sink;
	//! The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
	unordered_set<Pipeline *> parents;
	//! The dependencies of this pipeline
	unordered_set<Pipeline *> dependencies;
	//! The amount of completed dependencies (the pipeline can only be started after the dependencies have finished
	//! executing)
	std::atomic<idx_t> finished_dependencies;

	//! Whether or not the pipeline is finished executing
	bool finished;
	//! The current threads working on the pipeline
	std::atomic<idx_t> finished_tasks;
	//! The maximum amount of threads that can work on the pipeline
	idx_t total_tasks;

private:
	void ScheduleSequentialTask();
	bool ScheduleOperator(PhysicalOperator *op);
};

} // namespace duckdb



#include <queue>

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PhysicalOperatorState;
class ThreadContext;
class Task;

struct ProducerToken;

class Executor {
	friend class Pipeline;
	friend class PipelineTask;

public:
	Executor(ClientContext &context);
	~Executor();

	ClientContext &context;

public:
	void Initialize(unique_ptr<PhysicalOperator> physical_plan);
	void BuildPipelines(PhysicalOperator *op, Pipeline *parent);

	void Reset();

	vector<LogicalType> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

	//! Push a new error
	void PushError(std::string exception);

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);

private:
	unique_ptr<PhysicalOperator> physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;

	mutex executor_lock;
	//! The pipelines of the current query
	vector<unique_ptr<Pipeline>> pipelines;
	//! The producer of this query
	unique_ptr<ProducerToken> producer;
	//! Exceptions that occurred during the execution of the current query
	vector<string> exceptions;

	//! The amount of completed pipelines of the query
	std::atomic<idx_t> completed_pipelines;
	//! The total amount of pipelines in the query
	idx_t total_pipelines;

	unordered_map<ChunkCollection *, Pipeline *> delim_join_dependencies;
};
} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/profiler.hpp
//
//
//===----------------------------------------------------------------------===//





#include <chrono>

namespace duckdb {

//! The profiler can be used to measure elapsed time
class Profiler {
public:
	//! Starts the timer
	void Start() {
		finished = false;
		start = Tick();
	}
	//! Finishes timing
	void End() {
		end = Tick();
		finished = true;
	}

	//! Returns the elapsed time in seconds. If End() has been called, returns
	//! the total elapsed time. Otherwise returns how far along the timer is
	//! right now.
	double Elapsed() const {
		auto _end = finished ? end : Tick();
		return std::chrono::duration_cast<std::chrono::duration<double>>(_end - start).count();
	}

private:
	std::chrono::time_point<std::chrono::system_clock> Tick() const {
		return std::chrono::system_clock::now();
	}
	std::chrono::time_point<std::chrono::system_clock> start;
	std::chrono::time_point<std::chrono::system_clock> end;
	bool finished = false;
};
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/string_util.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {
/**
 * String Utility Functions
 * Note that these are not the most efficient implementations (i.e., they copy
 * memory) and therefore they should only be used for debug messages and other
 * such things.
 */
class StringUtil {
public:
	static bool CharacterIsSpace(char c) {
		return c == ' ' || c == '\t' || c == '\n' || c == '\v'  || c ==  '\f'  || c == '\r';
	}

	//! Returns true if the needle string exists in the haystack
	static bool Contains(const string &haystack, const string &needle);

	//! Returns true if the target string starts with the given prefix
	static bool StartsWith(string str, string prefix);

	//! Returns true if the target string <b>ends</b> with the given suffix.
	static bool EndsWith(const string &str, const string &suffix);

	//! Repeat a string multiple times
	static string Repeat(const string &str, const idx_t n);

	//! Split the input string based on newline char
	static vector<string> Split(const string &str, char delimiter);

	//! Join multiple strings into one string. Components are concatenated by the given separator
	static string Join(const vector<string> &input, const string &separator);

	//! Join multiple items of container with given size, transformed to string
	//! using function, into one string using the given separator
	template <typename C, typename S, typename Func>
	static string Join(const C &input, S count, const string &separator, Func f) {
		// The result
		std::string result;

		// If the input isn't empty, append the first element. We do this so we
		// don't need to introduce an if into the loop.
		if (count > 0) {
			result += f(input[0]);
		}

		// Append the remaining input components, after the first
		for (size_t i = 1; i < count; i++) {
			result += separator + f(input[i]);
		}

		return result;
	}

	//! Append the prefix to the beginning of each line in str
	static string Prefix(const string &str, const string &prefix);

	//! Return a string that formats the give number of bytes
	static string FormatSize(idx_t bytes);

	//! Convert a string to uppercase
	static string Upper(const string &str);

	//! Convert a string to lowercase
	static string Lower(const string &str);

	//! Format a string using printf semantics
	template <typename... Args> static string Format(const string fmt_str, Args... params) {
		return Exception::ConstructMessage(fmt_str, params...);
	}

	//! Split the input string into a vector of strings based on the split string
	static vector<string> Split(const string &input, const string &split);

	//! Remove the whitespace char in the left end of the string
	static void LTrim(string &str);
	//! Remove the whitespace char in the right end of the string
	static void RTrim(string &str);
	//! Remove the whitespace char in the left and right end of the string
	static void Trim(string &str);

	static string Replace(string source, const string &from, const string &to);
};
} // namespace duckdb






#include <stack>
#include <unordered_map>

namespace duckdb {
class PhysicalOperator;
class SQLStatement;

struct OperatorTimingInformation {
	double time = 0;
	idx_t elements = 0;

	OperatorTimingInformation(double time_ = 0, idx_t elements_ = 0) : time(time_), elements(elements_) {
	}
};

//! The OperatorProfiler measures timings of individual operators
class OperatorProfiler {
	friend class QueryProfiler;

public:
	OperatorProfiler(bool enabled);

	void StartOperator(PhysicalOperator *phys_op);
	void EndOperator(DataChunk *chunk);

private:
	void AddTiming(PhysicalOperator *op, double time, idx_t elements);

	//! Whether or not the profiler is enabled
	bool enabled;
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	std::stack<PhysicalOperator *> execution_stack;
	//! A mapping of physical operators to recorded timings
	unordered_map<PhysicalOperator *, OperatorTimingInformation> timings;
};

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler {
public:
	struct TreeNode {
		string name;
		string extra_info;
		vector<string> split_extra_info;
		OperatorTimingInformation info;
		vector<unique_ptr<TreeNode>> children;
		idx_t depth = 0;
	};

private:
	static idx_t GetDepth(QueryProfiler::TreeNode &node);
	unique_ptr<TreeNode> CreateTree(PhysicalOperator *root, idx_t depth = 0);

	static idx_t RenderTreeRecursive(TreeNode &node, vector<string> &render, vector<idx_t> &render_heights,
	                                 idx_t base_render_x = 0, idx_t start_depth = 0, idx_t depth = 0);
	static string RenderTree(TreeNode &node);

public:
	QueryProfiler() : automatic_print_format(ProfilerPrintFormat::NONE), enabled(false), running(false) {
	}

	void Enable() {
		enabled = true;
	}

	void Disable() {
		enabled = false;
	}

	bool IsEnabled() {
		return enabled;
	}

	void StartQuery(string query, SQLStatement &statement);
	void EndQuery();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	void Flush(OperatorProfiler &profiler);

	void StartPhase(string phase);
	void EndPhase();

	void Initialize(PhysicalOperator *root);

	string ToString() const;
	void Print();

	string ToJSON() const;
	void WriteToFile(const char *path, string &info) const;

	//! The format to automatically print query profiling information in (default: disabled)
	ProfilerPrintFormat automatic_print_format;
	//! The file to save query profiling information to, instead of printing it to the console (empty = print to
	//! console)
	string save_location;

private:
	//! Whether or not query profiling is enabled
	bool enabled;
	//! Whether or not the query profiler is running
	bool running;

	bool query_requires_profiling;

	//! The root of the query tree
	unique_ptr<TreeNode> root;
	//! The query string
	string query;

	//! The timer used to time the execution time of the entire query
	Profiler main_query;
	//! A map of a Physical Operator pointer to a tree node
	unordered_map<PhysicalOperator *, TreeNode *> tree_map;

	//! The timer used to time the individual phases of the planning process
	Profiler phase_profiler;
	//! A mapping of the phase names to the timings
	using PhaseTimingStorage = unordered_map<string, double>;
	PhaseTimingStorage phase_timings;
	using PhaseTimingItem = PhaseTimingStorage::value_type;
	//! The stack of currently active phases
	vector<string> phase_stack;

private:
	vector<PhaseTimingItem> GetOrderedPhaseTimings() const;

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(PhysicalOperatorType op_type);
};
} // namespace duckdb



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	TransactionContext(TransactionManager &transaction_manager)
	    : transaction_manager(transaction_manager), auto_commit(true), is_invalidated(false),
	      current_transaction(nullptr) {
	}
	~TransactionContext();

	Transaction &ActiveTransaction() {
		assert(current_transaction);
		return *current_transaction;
	}

	bool HasActiveTransaction() {
		return !!current_transaction;
	}

	void RecordQuery(string query);
	void BeginTransaction();
	void Commit();
	void Rollback();

	void SetAutoCommit(bool value) {
		auto_commit = value;
	}
	bool IsAutoCommit() {
		return auto_commit;
	}

	void Invalidate() {
		is_invalidated = true;
	}

private:
	TransactionManager &transaction_manager;
	bool auto_commit;
	bool is_invalidated;

	Transaction *current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb


#include <random>

namespace duckdb {
class Appender;
class Catalog;
class DuckDB;
class PreparedStatementData;
class Relation;
class BufferedFileWriter;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext {
public:
	ClientContext(DuckDB &database);

	//! Query profiler
	QueryProfiler profiler;
	//! The database that this client is connected to
	DuckDB &db;
	//! Data for the currently running transaction
	TransactionContext transaction;
	//! Whether or not the query is interrupted
	bool interrupted;
	//! Whether or not the ClientContext has been invalidated because the underlying database is destroyed
	bool is_invalidated = false;
	//! Lock on using the ClientContext in parallel
	std::mutex context_lock;
	//! The current query being executed by the client context
	string query;

	//! The query executor
	Executor executor;

	Catalog &catalog;
	unique_ptr<SchemaCatalogEntry> temporary_objects;
	unique_ptr<CatalogSet> prepared_statements;

	// Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;
	//! Force parallelism of small tables, used for testing
	bool force_parallelism = false;
	//! Output only the logical_opt explain output, used for optimization verification
	bool explain_output_optimized_only = false;
	//! The writer used to log queries (if logging is enabled)
	unique_ptr<BufferedFileWriter> log_query_writer;

	//! The random generator used by random(). Its seed value can be set by setseed().
	std::mt19937 random_engine;

public:
	Transaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	void Interrupt();
	//! Enable query profiling
	void EnableProfiling();
	//! Disable query profiling
	void DisableProfiling();

	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	unique_ptr<QueryResult> Query(string query, bool allow_stream_result);
	//! Fetch a query from the current result set (if any)
	unique_ptr<DataChunk> Fetch();
	//! Cleanup the result set (if any).
	void Cleanup();
	//! Invalidate the client context. The current query will be interrupted and the client context will be invalidated,
	//! making it impossible for future queries to run.
	void Invalidate();

	//! Get the table info of a specific table, or nullptr if it cannot be found
	unique_ptr<TableDescription> TableInfo(string schema_name, string table_name);
	//! Appends a DataChunk to the specified table. Returns whether or not the append was successful.
	void Append(TableDescription &description, DataChunk &chunk);
	//! Try to bind a relation in the current client context; either throws an exception or fills the result_columns
	//! list with the set of returned columns
	void TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	//! Execute a relation
	unique_ptr<QueryResult> Execute(shared_ptr<Relation> relation);

	//! Prepare a query
	unique_ptr<PreparedStatement> Prepare(string query);
	//! Execute a prepared statement with the given name and set of parameters
	unique_ptr<QueryResult> Execute(string name, vector<Value> &values, bool allow_stream_result = true,
	                                string query = string());
	//! Removes a prepared statement from the set of prepared statements in the client context
	void RemovePreparedStatement(PreparedStatement *statement);

	void RegisterAppender(Appender *appender);
	void RemoveAppender(Appender *appender);

	//! Register function in the temporary schema
	void RegisterFunction(CreateFunctionInfo *info);

private:
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	string VerifyQuery(string query, unique_ptr<SQLStatement> statement);

	void InitialCleanup();
	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal();
	string FinalizeQuery(bool success);
	//! Internal fetch, does not lock. Caller must hold the context_lock.
	unique_ptr<DataChunk> FetchInternal();
	//! Internally execute a set of SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatements(const string &query, vector<unique_ptr<SQLStatement>> &statements,
	                                      bool allow_stream_result);
	//! Internally prepare and execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatement(const string &query, unique_ptr<SQLStatement> statement,
	                                     bool allow_stream_result);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	unique_ptr<PreparedStatementData> CreatePreparedStatement(const string &query, unique_ptr<SQLStatement> statement);
	//! Internally execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> ExecutePreparedStatement(const string &query, PreparedStatementData &statement,
	                                                 vector<Value> bound_values, bool allow_stream_result);
	//! Call CreatePreparedStatement() and ExecutePreparedStatement() without any bound values
	unique_ptr<QueryResult> RunStatementInternal(const string &query, unique_ptr<SQLStatement> statement,
	                                             bool allow_stream_result);

	template <class T> void RunFunctionInTransaction(T &&fun);

private:
	idx_t prepare_count = 0;
	//! The currently opened StreamQueryResult (if any)
	StreamQueryResult *open_result = nullptr;
	//! Prepared statement objects that were created using the ClientContext::Prepare method
	unordered_set<PreparedStatement *> prepared_statement_objects;
	//! Appenders that were attached to this client context
	unordered_set<Appender *> appenders;
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

//! Function used for determining the return type of a table producing function
typedef unique_ptr<FunctionData> (*table_function_bind_t)(ClientContext &context, vector<Value> &inputs,
                                                          unordered_map<string, Value> &named_parameters,
                                                          vector<LogicalType> &return_types, vector<string> &names);
//! Type used for table-returning function
typedef void (*table_function_t)(ClientContext &context, vector<Value> &input, DataChunk &output,
                                 FunctionData *dataptr);
//! Type used for final (cleanup) function
typedef void (*table_function_final_t)(ClientContext &context, FunctionData *dataptr);

class TableFunction : public SimpleFunction {
public:
	TableFunction(string name, vector<LogicalType> arguments, table_function_bind_t bind, table_function_t function,
	              table_function_final_t final = nullptr, bool supports_projection = false)
	    : SimpleFunction(name, move(arguments)), bind(bind), function(function), final(final),
	      supports_projection(supports_projection) {
	}
	TableFunction(vector<LogicalType> arguments, table_function_bind_t bind, table_function_t function,
	              table_function_final_t final = nullptr, bool supports_projection = false)
	    : TableFunction(string(), move(arguments), bind, function, final, supports_projection) {
	}

	//! The bind function
	table_function_bind_t bind;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;
	//! Supported named parameters by the function
	unordered_map<string, LogicalType> named_parameters;
	//! Whether or not the table function supports projection
	bool supports_projection;

	string ToString() {
		return Function::CallToString(name, arguments);
	}
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_info.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/parse_info.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

struct ParseInfo {
	virtual ~ParseInfo() {
	}
};

} // namespace duckdb



namespace duckdb {

enum class OnCreateConflict : uint8_t {
	// Standard: throw error
	ERROR,
	// CREATE IF NOT EXISTS, silently do nothing on conflict
	IGNORE,
	// CREATE OR REPLACE
	REPLACE
};

struct CreateInfo : public ParseInfo {
	CreateInfo(CatalogType type, string schema = DEFAULT_SCHEMA)
	    : type(type), schema(schema), on_conflict(OnCreateConflict::ERROR), temporary(false) {
	}
	virtual ~CreateInfo() {
	}

	//! The to-be-created catalog type
	CatalogType type;
	//! The schema name of the entry
	string schema;
	//! What to do on create conflict
	OnCreateConflict on_conflict;
	//! Whether or not the entry is temporary
	bool temporary;
};

} // namespace duckdb



namespace duckdb {

struct CreateFunctionInfo : public CreateInfo {
	CreateFunctionInfo(CatalogType type) : CreateInfo(type) {
		assert(type == CatalogType::SCALAR_FUNCTION_ENTRY || type == CatalogType::AGGREGATE_FUNCTION_ENTRY || type == CatalogType::TABLE_FUNCTION_ENTRY);
	}

	//! Function name
	string name;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_set.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {

template <class T> class FunctionSet {
public:
	FunctionSet(string name) : name(name) {
	}

	//! The name of the function set
	string name;
	//! The set of functions
	vector<T> functions;

public:
	void AddFunction(T function) {
		function.name = name;
		functions.push_back(function);
	}
};

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	ScalarFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	AggregateFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	TableFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

} // namespace duckdb


namespace duckdb {

struct CreateTableFunctionInfo : public CreateFunctionInfo {
	CreateTableFunctionInfo(TableFunctionSet set)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(move(set.functions)) {
		this->name = set.name;
	}
	CreateTableFunctionInfo(TableFunction function)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY) {
		this->name = function.name;
		functions.push_back(move(function));
	}

	//! The table functions
	vector<TableFunction> functions;
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_copy_function_info.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/copy_function.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/copy_info.hpp
//
//
//===----------------------------------------------------------------------===//








namespace duckdb {

struct CopyInfo : public ParseInfo {
	CopyInfo()
	    : schema(DEFAULT_SCHEMA) {
	}

	//! The schema name to copy to/from
	string schema;
	//! The table name to copy to/from
	string table;
	//! List of columns to copy to/from
	vector<string> select_list;
	//! The file path to copy to/from
	string file_path;
	//! Whether or not this is a copy to file (false) or copy from a file (true)
	bool is_from;

	//! The file format of the external file
	string format;

	unordered_map<string, vector<Value>> options;
};

} // namespace duckdb


namespace duckdb {
class ExecutionContext;

struct LocalFunctionData {
	virtual ~LocalFunctionData() {
	}
};

struct GlobalFunctionData {
	virtual ~GlobalFunctionData() {
	}
};

typedef unique_ptr<FunctionData> (*copy_to_bind_t)(ClientContext &context, CopyInfo &info, vector<string> &names,
                                                   vector<LogicalType> &sql_types);
typedef unique_ptr<LocalFunctionData> (*copy_to_initialize_local_t)(ClientContext &context, FunctionData &bind_data);
typedef unique_ptr<GlobalFunctionData> (*copy_to_initialize_global_t)(ClientContext &context, FunctionData &bind_data);
typedef void (*copy_to_sink_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                               LocalFunctionData &lstate, DataChunk &input);
typedef void (*copy_to_combine_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                  LocalFunctionData &lstate);
typedef void (*copy_to_finalize_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate);

typedef unique_ptr<FunctionData> (*copy_from_bind_t)(ClientContext &context, CopyInfo &info,
                                                     vector<string> &expected_names,
                                                     vector<LogicalType> &expected_types);
typedef unique_ptr<GlobalFunctionData> (*copy_from_initialize_t)(ClientContext &context, FunctionData &bind_data);
typedef void (*copy_from_get_chunk_t)(ExecutionContext &context, GlobalFunctionData &gstate, FunctionData &bind_data,
                                      DataChunk &chunk);

class CopyFunction : public Function {
public:
	CopyFunction(string name)
	    : Function(name), copy_to_bind(nullptr), copy_to_initialize_local(nullptr), copy_to_initialize_global(nullptr),
	      copy_to_sink(nullptr), copy_to_combine(nullptr), copy_to_finalize(nullptr), copy_from_bind(nullptr) {
	}

	copy_to_bind_t copy_to_bind;
	copy_to_initialize_local_t copy_to_initialize_local;
	copy_to_initialize_global_t copy_to_initialize_global;
	copy_to_sink_t copy_to_sink;
	copy_to_combine_t copy_to_combine;
	copy_to_finalize_t copy_to_finalize;

	copy_from_bind_t copy_from_bind;
	copy_from_initialize_t copy_from_initialize;
	copy_from_get_chunk_t copy_from_get_chunk;

	string extension;
};

} // namespace duckdb


namespace duckdb {

struct CreateCopyFunctionInfo : public CreateInfo {
	CreateCopyFunctionInfo(CopyFunction function) : CreateInfo(CatalogType::COPY_FUNCTION), function(function) {
		this->name = function.name;
	}

	//! Function name
	string name;
	//! The table function
	CopyFunction function;
};

} // namespace duckdb
