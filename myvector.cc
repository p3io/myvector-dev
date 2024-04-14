/*  Copyright (c) 2024 - p3io.in / shiyer22@gmail.com */
/*
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "hnswlib.h"

#include <regex>
#include <algorithm>
#include <string>
#include <iomanip>
#include <memory>
#include <thread>
#include <vector>
#include <list>
#include <set>
#include <mutex>
#include <shared_mutex>

#include <unistd.h>

#define debug_print(...) my_plugin_log_message(&gplugin, MY_INFORMATION_LEVEL, __VA_ARGS__)

using namespace std;

#include "plugin/myvector/myvector.h"
#include "mysql.h" 
#include "mysql/plugin.h"
#include "mysql/udf_registration_types.h"
#include "mysql/service_my_plugin_log.h"
#include "my_checksum.h"

#include <mysql/components/component_implementation.h>
#include <mysql/components/services/mysql_string.h>
#include <mysql/components/services/udf_metadata.h>
#include <mysql/components/my_service.h>
 
extern REQUIRES_SERVICE_PLACEHOLDER(mysql_udf_metadata);
extern REQUIRES_SERVICE_PLACEHOLDER(mysql_string_converter);
extern REQUIRES_SERVICE_PLACEHOLDER(mysql_string_factory);

#include <mysql/service_plugin_registry.h>

#define SET_UDF_ERROR_AND_RETURN(...) \
  { \
    my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, __VA_ARGS__); \
    *error = 1; \
    return (result); \
  }

extern my_service<SERVICE_TYPE(mysql_udf_metadata)> *h_udf_metadata_service;

class AbstractVectorIndex;

/* MyVector only supports INT/BIGINT key type. 2 options for users :-

   1. Create the table with a INT primary key.
   OR
   2. If INT variant primary key is not possible, user can add a
      AUTO INCREMENT with UNIQUE index column.
*/

typedef size_t KeyTypeInteger;


/* v1 uses 4-byte floats for representing vector dimensions.
   Just by templatizing all references to FP32, MyVector can support
   FP16, 8-bit SQ etc in v2 :-

      Replace all references to FP32 with T.
      Add template<class T> before the functions/classes.
 */
typedef float       FP32;

typedef void*       VectorPtr;

/* Each serialized vector has 4 bytes of metadata.
 * Byte 1 - MyVector vector format version
 * Byte 2 - Native datatype of vector elements. e.g FP32, FP16 etc
 * Byte 3 - Unused
 * Byte 4 - Unused
 */

static const unsigned int MYVECTOR_VERSION_V1  = 0x01;

static const unsigned int MYVECTOR_VECTOR_FP32 = 0x01;

static const unsigned int MYVECTOR_VECTOR_FP16 = 0x02;

static const unsigned int MYVECTOR_VECTOR_BV   = 0x04;

#define MYVECTOR_V1_FP32_METADATA \
   ((MYVECTOR_VECTOR_FP32 << 8) | MYVECTOR_VERSION_V1)

#define MYVECTOR_V1_BV_METADATA \
    ((MYVECTOR_VECTOR_BV  << 8) | MYVECTOR_VERSION_V1)

/* Maximum length of string that can be passed to myvector_construct(). */
  
static const unsigned int MYVECTOR_CONSTRUCT_MAX_LEN = 128000;

/* Maximum buffer space for return value of myvector_display().
   e.g 3072 dimensions x 23 characters for each dimension = 70kb string
 */
static const unsigned int MYVECTOR_DISPLAY_MAX_LEN   = 128000;

/* Default precision for float output in myvector_display() */
static const unsigned int MYVECTOR_DISPLAY_DEF_PREC  = 7;

/* Overhead per MYVECTOR column value - 4 bytes for checksum and
   4 bytes for metadata
 */ 
static const unsigned int MYVECTOR_COLUMN_EXTRA_LEN  = 8;

/* Default number of neighbours to return by myvector_ann_set() */
static const unsigned int MYVECTOR_DEFAULT_ANN_RETURN_COUNT = 10;

/* Max number of neighbours that can be retrieved in single myvector_ann_set() */
static const unsigned int MYVECTOR_MAX_ANN_RETURN_COUNT = 10000;

/* Basic check for validity of index last update timestamp > '01-01-2024' */
static const unsigned long MYVECTOR_MIN_VALID_UPDATE_TS = 1704047400;

/* Parallel threads are started after 100K vectors + keys have been batch'ed up */
static const unsigned int HNSW_PARALLEL_BUILD_UNIT_SIZE = 100000;

/* Bit packing for Binary Vectors */
static const unsigned int BITS_PER_BYTE                 = 8;

extern MYSQL_PLUGIN gplugin;
extern char *myvector_index_dir;

char *latin1 = const_cast<char *>("latin1");

const set<string> MYVECTOR_INDEX_TYPES{"KNN", "HNSW", "HNSW_BV"};

/* A generic key-value map for options */
typedef unordered_map<string, string> OptionsMap;

inline bool isValidIndexType(string &indextype) {
  return (MYVECTOR_INDEX_TYPES.find(indextype) !=
            MYVECTOR_INDEX_TYPES.end());
}

inline int MyVectorStorageLength(int dim)
{
  return (dim * sizeof(FP32)) + MYVECTOR_COLUMN_EXTRA_LEN;
}

inline int MyVectorDimFromStorageLength(int length)
{
  return (length - MYVECTOR_COLUMN_EXTRA_LEN) / sizeof(FP32);
}

inline int MyVectorBVStorageLength(int dim)
{
  return (dim / BITS_PER_BYTE) + MYVECTOR_COLUMN_EXTRA_LEN;
}

inline int MyVectorBVDimFromStorageLength(int length)
{
  return (length - MYVECTOR_COLUMN_EXTRA_LEN) * BITS_PER_BYTE;
}

/* Trim leading & trailing spaces */
inline string lrtrim(const string &str)
{
 string ret =
   regex_replace(str, regex("^ +| +$|( ) +"), "$1");
 
 return ret;
}

/* Split a string at comma and return ordered list */
inline void split(const string &str, vector<string> &out)
{
  static const regex re("[^,]+");

  sregex_iterator current(str.begin(), str.end(), re);
  sregex_iterator end;
  while (current != end) {
    string val = lrtrim((*current).str());
    out.push_back(val);
    current++;
  }
}

/* Helper class to manage vector index options as k-v map.
 * e.g type=HNSW,dim=1536,size=1000000,M=64,ef=100
 */
class MyVectorOptions {
  private:
    /* Returns true on success, false on format error */
    bool parseKV(const char *line)
    {
      /* e.g options list-MYVECTOR(type=hnsw,dim=50,size=4000000,M=64,ef=100) */
      const char *ptr1 = line;
      if (strchr(line,'|')) { // start marker
        ptr1 = strchr(line,'|');
        ptr1++;
      }
      string          sline(ptr1);
      vector<string>  listoptions;

      split(sline, listoptions);

      for (auto s : listoptions) {
        size_t eq = s.find_first_of('=');
        if (eq == string::npos) return false;

        string k = s.substr(0, eq);
        string v = s.substr(eq+1);

        k = lrtrim(k);
        v = lrtrim(v);

        if (!k.length() || !v.length()) return false;

        setOption(k, v);
      }

      return true;
    } /* parseKV */

  public:
    MyVectorOptions(const string &options) {
      m_valid = false;
      if (parseKV(options.c_str())) m_valid = true;
    }

    bool        isValid() const { return m_valid; }
    void        setOption(const string &name, const string &val)
      { m_options[name] = val; }
    string getOption(const string &name)
      { string ret = "";
        if (m_options.find(name) != m_options.end()) ret = m_options[name];
        return ret;
      }
           
  private:
    OptionsMap m_options;
    bool       m_valid;
};

/* Compute L2/Eucliean squared distance via optimized function from hnswlib */
double computeL2Distance(FP32 *v1, FP32 *v2, int dim)
{
  hnswlib::L2Space         sp(dim);
  hnswlib::DISTFUNC<FP32>  distfn;
  double dist = 0.0;
  size_t sdim = dim;

  if (v1 && v2 && dim) {
    distfn = sp.get_dist_func();
    dist   = distfn(v1,v2,&sdim);
  }

  return dist;
}

/* Compute InnerProduct distance via optimized function from hnswlib */
double computeIPDistance(FP32 *v1, FP32 *v2, int dim)
{
  hnswlib::InnerProductSpace sp(dim);
  hnswlib::DISTFUNC<FP32>    distfn;
  double dist = 0.0;
  size_t sdim = dim;

  if (v1 && v2 && dim) {
    distfn = sp.get_dist_func();
    dist   = distfn(v1,v2,&sdim);
  }

  return dist;
}

/* Compute Cosine distance using standard formula */
double computeCosineDistance(FP32 *v1, FP32 *v2, int dim)
{
  double dist = 0.0, v1v2 = 0.0, normv1 = 0.0, normv2 = 0.0;

  for (int i = 0; i < dim; i++) {
    v1v2   += (v1[i] * v2[i]);
    normv1 += (v1[i] * v1[i]);
    normv2 += (v2[i] * v2[i]);
  }
  double t = (sqrt(normv1 * normv2));
  if (t)
    dist = (double)v1v2 / t;

  return (1 - dist);
}
float HammingDistanceFn(const void* __restrict pVect1, const void* __restrict pVect2, const void* __restrict qty_ptr) {


    size_t qty = *((size_t*)qty_ptr); // dimension of the vector
    float dist = 0;
    unsigned long ldist = 0;
    unsigned long* a = (unsigned long*)pVect1;
    unsigned long* b = (unsigned long*)pVect2;

    /* Hamming Distance between 2 byte sequences - Number of bit positions
     * matching/different in both the sequences. In the plugin, we calculate
     * the diff'ing bit positions and that is the distance. Thus smaller
     * distance implies the vectors are 'nearer'/'similar' to each other.
     */
    // TODO - Use AVX2/AVX512 or try __builtin_popcountll()
    size_t iter = (qty / (sizeof(unsigned long) * BITS_PER_BYTE));
    for (size_t i = 0; i < iter; i++) {
       unsigned long res = (*a ^ *b); a++; b++;
#ifdef SLOW_CODE
       while (res > 0) {
         ldist += (res & 1);
         res >>= 1;
       }
#endif
       ldist += __builtin_popcountll(res);
    }

    dist = ldist;
    // debug_print("hamming distance return  = %lu", ldist);
    return dist;
}

/* HammingBinaryVectorSpace : Implement hnswlib's SpaceInterface for
 * Hamming Distance based HNSW index for Binary Vectors.
 */
class HammingBinaryVectorSpace : public hnswlib::SpaceInterface<float> {
    hnswlib::DISTFUNC<float> fstdistfunc_;
    size_t data_size_;
    size_t dim_;

 public:
    HammingBinaryVectorSpace(size_t dim) {
        fstdistfunc_ = HammingDistanceFn;
        dim_         = dim;
        data_size_   = (dim / BITS_PER_BYTE); // 1 bit per dimension
    }

    size_t get_data_size() {
        return data_size_;
    }

    hnswlib::DISTFUNC<float> get_dist_func() {
        return fstdistfunc_;
    }

    void *get_dist_func_param() {
        return &dim_;
    }

    ~HammingBinaryVectorSpace() {}
};

/* Interface for various types of vector indexes. Initial design is based
 * on 2 index types - 1) KNN in-memory using vector<> and priority_queue<>
 * 2) HNSW in-memory with persistence from hnswlib.
 */
class AbstractVectorIndex
{
  public:
          virtual ~AbstractVectorIndex() {}

          virtual string getName() = 0;

          virtual string getType() = 0;

          virtual int         getDimension()
          { return 0; }

          virtual bool        supportsIncrUpdates()
          { return false; }

          virtual bool        supportsPersist()
          { return false; }

          virtual bool        supportsConcurrentUpdates()
          { return false; }

          virtual bool        isReady()
          { return false; }

          virtual bool        isDirty()
          { return false; }

          virtual string  getStatus()
          { return getName() + "<Status>"; }

          virtual bool loadIndex(const string &path)  = 0;

          virtual bool saveIndex(const string &path)  = 0;
          
          virtual bool dropIndex(const string& path)  = 0;

          virtual bool initIndex()                    = 0;

          virtual bool closeIndex()                   = 0;

          /* searchVectorNN - search and return 'n' Nearest Neighbours */
          virtual bool searchVectorNN(VectorPtr qvec, int dim,
                          vector<KeyTypeInteger> &nnkeys,
                          int n) = 0;

          /* insertVectortor - insert a vector into the index */
          virtual bool insertVector(VectorPtr vec, int dim, KeyTypeInteger id) = 0;

          /* startParallelBuild - User has initiated parallel index build/rebuild */
          virtual bool startParallelBuild(int nthreads) = 0;

          /* setUpdateTs - timestamp when the last index build/refresh was started */
          virtual void setUpdateTs(unsigned long ts) = 0;

          /* getUpdateTs - get timestamp when the last index build/refresh was started */
          virtual unsigned long getUpdateTs() = 0;

          /* getRowCount - get number of vectors present in the index */
          virtual unsigned long getRowCount() = 0;

          void lockShared()      { m_mutex.lock_shared(); }
          void lockExclusive()   { m_mutex.lock(); }

          void unlockShared()    { m_mutex.unlock_shared(); }
          void unlockExclusive() { m_mutex.unlock(); }

          shared_mutex& mutex()  { return m_mutex; }
  private:
    mutable shared_mutex m_mutex;
};

/* KNNIndex - a vector index type that implements brute-force KNN search in
 * the MyVector plugin. This index type could possibly be faster than SQL
 * performing ORDER BY myvector_distance(...) [as long as all vectors fit
 * in memory].
 */
class KNNIndex : public AbstractVectorIndex
{
  public:
          KNNIndex(const string &name, const string &options);

          ~KNNIndex() { }

          /* Next 3 are no-op in the KNN in-memory index */
          bool saveIndex(const string &path);
          
          bool loadIndex(const string &path);

          bool dropIndex(const string &path);
          
          bool initIndex();

          bool closeIndex();
          
          string getName() { return m_name; }
          string getType() { return "KNN"; }

          bool searchVectorNN(VectorPtr qvec, int dim,
                          vector<KeyTypeInteger> &keys, int n);
          bool insertVector(VectorPtr vec, int dim, KeyTypeInteger id);

          bool        supportsIncrUpdates()
          { return true; }

          bool        supportsPersist()
          { return false; }

          bool        supportsConcurrentUpdates()
          { return false; }

          bool        isReady()
          { return true; }

          bool        isDirty()
          { return false; }

          int getDimension() { return m_dim; }
          
          bool startParallelBuild(int nthreads)  { return false; }

          void setUpdateTs(unsigned long ts) { m_updateTs = ts; }

          unsigned long getUpdateTs()        { return m_updateTs; }
          
          unsigned long getRowCount()        { return m_n_rows; }

  private:
          string          m_name;
          string          m_options;
          int             m_dim;
          unsigned long   m_updateTs;
          MyVectorOptions m_optionsMap;

          atomic<unsigned long>    m_n_rows{0};
          atomic<unsigned long>    m_n_searches{0};

          /* The in-memory data store for the vectors */
          vector< pair<vector<FP32>, KeyTypeInteger> >  m_vectors;
};

KNNIndex::KNNIndex(const string &name, const string &options) 
  : m_name(name), m_options(options), m_optionsMap(options), m_updateTs(0)  {
  m_dim        = atoi(m_optionsMap.getOption("dim").c_str());
}

/* Brute-force, exact search KNN implemented using in-memory vector<> and
 * priority queue. Potentially faster than SELECT ... ORDER BY myvector_distance()
 */
bool KNNIndex::searchVectorNN(VectorPtr qvec, int dim, vector<KeyTypeInteger> &keys, int n)
{
  priority_queue< pair<FP32,KeyTypeInteger> > pq;
  keys.clear();

  /* Use priority queue to find out 'n' neighbours with least distance */
  for (auto row : m_vectors) {
    vector<FP32> &a = row.first;
    double dist = computeCosineDistance((FP32 *)qvec, a.data(), dim);
    if (pq.size() < n)
      pq.push({dist, row.second});
    else {
      auto top = pq.top();
      if (dist < top.first) {
        pq.pop();
        pq.push({dist,row.second});
      }
    }
  } /* for */

  while (pq.size()) {
    auto r = pq.top(); pq.pop();
    keys.push_back(r.second);
  }

  reverse(keys.begin(), keys.end()); // nearest to farthest

  m_n_searches++;
  return true;
}

/* insertVector - just stash the vector into in-memory vector<> collection */
bool KNNIndex::insertVector(VectorPtr vec, int dim, KeyTypeInteger id)
{
  FP32 *fvec = static_cast<FP32 *>(vec);
  vector<FP32> row(fvec, fvec + dim);
  m_vectors.push_back({row, id}); // simple, demo purpose index

  m_n_rows++;
  
  return true;
}

bool KNNIndex::saveIndex([[maybe_unused]]const string &path) {

  my_plugin_log_message(&gplugin, MY_WARNING_LEVEL,
    "KNN Memory Index (%s) - Save Index to disk is no-op", m_name.c_str());

  return true;
}

bool KNNIndex::dropIndex([[maybe_unused]]const string &path) {

  my_plugin_log_message(&gplugin, MY_WARNING_LEVEL,
    "KNN Memory Index (%s) - Drop Index is no-op", m_name.c_str());

  return true;

}

bool KNNIndex::loadIndex([[maybe_unused]]const string &path) {
  my_plugin_log_message(&gplugin, MY_WARNING_LEVEL,
    "KNN Memory Index (%s) - Load Index is no op", m_name.c_str());

  return true;
}

bool KNNIndex::initIndex() {
  debug_print("KNN Memory Index (%s) - initIndex()", m_name.c_str());
  m_vectors.clear();
  m_n_rows = 0;
  m_n_searches = 0;
  return true;
}

bool KNNIndex::closeIndex() {
  return true;
}


class HNSWMemoryIndex : public AbstractVectorIndex
{
  public:
          HNSWMemoryIndex(const string &name, const string &options);

          ~HNSWMemoryIndex();

          bool saveIndex(const string &path);

          bool loadIndex(const string &path);

          bool dropIndex(const string &path);

          bool initIndex();

          bool closeIndex();

          string getName() { return m_name; }

          string getType() { return m_type; }

          bool searchVectorNN(VectorPtr qvec, int dim, vector<KeyTypeInteger> &keys,
                          int n);
          
          bool insertVector(VectorPtr vec, int dim, KeyTypeInteger id);


          int getDimension()                  { return m_dim; }

          void setUpdateTs(unsigned long ts)  { m_updateTs = ts; }

          unsigned long getUpdateTs()         { return m_updateTs; }
          
          unsigned long getRowCount()         { return m_n_rows; }
          
          bool startParallelBuild(int nthreads);

  private:
    string        m_name;
    string        m_type;
    string        m_options;
    MyVectorOptions m_optionsMap;
    unsigned long   m_updateTs;

    int         m_dim;
    int         m_ef_construction;
    int         m_ef_search;
    int         m_M;
    int         m_size;
          
    hnswlib::HierarchicalNSW<FP32> *m_alg_hnsw = nullptr;
    //hnswlib::L2Space* m_space = nullptr;
     hnswlib::SpaceInterface<float>* m_space = nullptr;


    atomic<unsigned long>    m_n_rows{0};
    atomic<unsigned long>    m_n_searches{0};

    bool                     m_isDirty;

    /* Temporary store for multi-threaded, parallel index build */
    vector<char>           m_batch;
    vector<KeyTypeInteger> m_batchkeys;
    bool                   m_isParallelBuild{false};

    bool                   flushBatchParallel();
    bool                   flushBatchSerial();

    hnswlib::SpaceInterface<float>* getSpace(size_t dim);

    int                    m_threads;

};


HNSWMemoryIndex::HNSWMemoryIndex(const string &name, const string &options)
  : m_name(name), m_options(options), m_optionsMap(options), m_updateTs(0)
{
  m_dim               = atoi(m_optionsMap.getOption("dim").c_str());
  m_size              = atoi(m_optionsMap.getOption("size").c_str());
  m_ef_construction   = atoi(m_optionsMap.getOption("ef").c_str());
  m_ef_search         = m_ef_construction;
  m_M                 = atoi(m_optionsMap.getOption("M").c_str());
  m_type              = m_optionsMap.getOption("type"); // Supports HNSW and HNSW_BV

  if (m_optionsMap.getOption("ef_search").length())
    m_ef_search = atoi(m_optionsMap.getOption("ef_search").c_str());

  debug_print("hnsw index params %s %s  %d %d %d %d %d", name.c_str(), m_type.c_str(), m_dim,
               m_size, m_ef_construction, m_ef_search, m_M);

}

HNSWMemoryIndex::~HNSWMemoryIndex() {
  if (m_alg_hnsw) delete m_alg_hnsw;
  if (m_space)    delete m_space;
}

bool HNSWMemoryIndex::initIndex()
{
  debug_print("hnsw initIndexO %p %s %d %d %d %d %d", this, m_name.c_str(), m_dim,
               m_size, m_ef_construction, m_ef_search, m_M);
  //m_space    = new hnswlib::L2Space(m_dim);
  m_space    = getSpace(m_dim);
  m_alg_hnsw = new hnswlib::HierarchicalNSW<FP32>(m_space, m_size,
                     m_M, m_ef_construction);

  m_alg_hnsw->setEf(m_ef_search);

  m_n_rows = 0;
  m_n_searches = 0;

  return true;
}

bool HNSWMemoryIndex::saveIndex(const string &path)
{
  if (!m_isDirty) {
    my_plugin_log_message(&gplugin, MY_WARNING_LEVEL, 
      "HNSW index %s is not updated, save not required", m_name.c_str());
    return true;
  }

  if (m_isParallelBuild) {
     flushBatchSerial(); // last batch, maybe small
  }
  
  string statusf = path + "/" + m_name + ".hnsw.index" + ".safe";
  unlink(statusf.c_str());

  string filename = path + "/" + m_name + ".hnsw.index";
 
  // hnswlib method for full write/rewrite. Expect 10GB to take 10 secs. 
  m_alg_hnsw->saveIndex(filename);

  ofstream sf(statusf, ios::out | ios::binary);
  sf.write((char *)&m_updateTs, sizeof(m_updateTs));
  sf.close();

  m_isDirty = false;
  m_isParallelBuild = false;

  return true;
}

bool HNSWMemoryIndex::loadIndex(const string &path)
{
  if (m_alg_hnsw) delete m_alg_hnsw;
  if (m_space)    delete m_space;

  //m_space =  new hnswlib::L2Space(m_dim);
  m_space    = getSpace(m_dim);
  
  string indexfile = path + "/" + m_name + ".hnsw.index";
  string statusf   = path + "/" + m_name + ".hnsw.index" + ".safe";
  bool   readindex = false;
 
  ifstream f(statusf, ios::in | ios :: binary);
  if (f.good()) {
    ifstream f2(indexfile);
    if (f2.good())
      readindex = true;
    else
      my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
        "Index file %s not found.", indexfile.c_str());
  }
  else {
    my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
      "Index status file %s not found.", statusf.c_str());
  }
  if (!readindex) {
    return false;
  }

  unsigned long lastupdatets = 0;
  f.read((char *)&lastupdatets, sizeof(lastupdatets));

  if (lastupdatets < MYVECTOR_MIN_VALID_UPDATE_TS) {
    my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
      "Invalid last update timestamp %lu in %s.", lastupdatets, statusf.c_str());
    return false;
  }

  debug_print("Loading HNSW index %s from %s, last update timestamp = %lu",
              m_name.c_str(), indexfile.c_str(), lastupdatets);

  /* hnswlib throws std::runtime_error for errors */
  try {
    m_alg_hnsw = new hnswlib::HierarchicalNSW<FP32>(m_space, indexfile);
  } catch (std::runtime_error &e) {
    my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
      "Error loading hnsw index (%s) from file : %s", m_name.c_str(), e.what());
    return false;
  }


  m_alg_hnsw->setEf(m_ef_search);

  setUpdateTs(lastupdatets); // in-memory

  return true;
}

bool HNSWMemoryIndex::dropIndex(const string &path)
{
  /* Force drop index - delete files and free memory */
  string indexfile = path + "/" + m_name + ".hnsw.index";
  unlink(indexfile.c_str());
  
  string statusfile = indexfile + ".safe";
  unlink(statusfile.c_str());

  if (m_alg_hnsw) delete m_alg_hnsw;
  m_alg_hnsw = nullptr;

  if (m_space)    delete m_space;
  m_space    = nullptr;

  return true;
}

bool HNSWMemoryIndex::closeIndex() {
  return true;
}
    
hnswlib::SpaceInterface<float>* HNSWMemoryIndex::getSpace(size_t dim) {
  debug_print("getSpace for %s",m_type.c_str());
  if (m_type == "HNSW") 
    return new hnswlib::L2Space(m_dim);
  else if (m_type == "HNSW_BV")
    return new HammingBinaryVectorSpace(m_dim);

  return nullptr;
}

bool HNSWMemoryIndex::searchVectorNN(VectorPtr qvec, int dim,
                          vector<KeyTypeInteger> &keys,
                          int n) {

  priority_queue<pair<FP32, hnswlib::labeltype>> result =
                  m_alg_hnsw->searchKnn(qvec, n);

  keys.clear();
  while (!result.empty()) {
    keys.push_back(result.top().second);
    result.pop();
  }

  reverse(keys.begin(), keys.end()); // nearest to farthest
  m_n_searches++;
  return true;
}


// Parallel HNSW index load (hnswlib:example_search_mt.cpp)
template<class Function>
inline void ParallelFor(size_t start, size_t end, size_t numThreads, Function fn) {
    debug_print("Entered ParallelFor %lu %lu t=%lu", start, end, numThreads);
    if (numThreads <= 0) {
        numThreads = thread::hardware_concurrency();
    }

    if (numThreads == 1) {
        for (size_t id = start; id < end; id++) {
            fn(id, 0);
        }
    } else {
        vector<thread> threads;
        atomic<size_t> current(start);

        // keep track of exceptions in threads
        // https://stackoverflow.com/a/32428427/1713196
        exception_ptr lastException = nullptr;
        mutex lastExceptMutex;

        for (size_t threadId = 0; threadId < numThreads; ++threadId) {
            threads.push_back(thread([&, threadId] {
                while (true) {
                    size_t id = current.fetch_add(1);

                    if (id >= end) {
                        break;
                    }

                    try {
                        fn(id, threadId);
                    } catch (...) {
                        unique_lock<mutex> lastExcepLock(lastExceptMutex);
                        lastException = current_exception();
                        /*
                         * This will work even when current is the largest value that
                         * size_t can fit, because fetch_add returns the previous value
                         * before the increment (what will result in overflow
                         * and produce 0 instead of current + 1).
                         */
                        current = end;
                        break;
                    }
                }
            }));
        }
        for (auto &thread : threads) {
            thread.join();
        }
        if (lastException) {
            rethrow_exception(lastException);
        }
    }
}

bool HNSWMemoryIndex::startParallelBuild(int nthreads)
{
  m_batch.clear(); m_batchkeys.clear();
  m_isParallelBuild = true;
  m_threads = nthreads;
  return true;
}

bool HNSWMemoryIndex::flushBatchSerial() {
  debug_print("flushBatchSerial %lu", m_batchkeys.size());
  for (unsigned int i = 0; i < m_batchkeys.size(); i++) {
    m_alg_hnsw->addPoint((void *)&(m_batch[i * m_space->get_data_size()]), m_batchkeys[i]);
  }
  return true;
}

bool HNSWMemoryIndex::flushBatchParallel() {
    debug_print("Entered flushBatchParallel for (%s), nthreads = %d, sz = %u",
                m_name.c_str(), m_threads, m_batchkeys.size());
    /* Add data to index - HNSW multi-threaded example from 
     * hnswlib:example_search_mt.cpp.
     */
    ParallelFor(0, m_batchkeys.size(), m_threads, [&](size_t row, size_t threadId) {
        m_alg_hnsw->addPoint((void*)&(m_batch[row * m_space->get_data_size()]), m_batchkeys[row]);
    });

    m_batch.clear();
    m_batchkeys.clear();

    return true;
}

bool HNSWMemoryIndex::insertVector(VectorPtr vec, int dim, KeyTypeInteger id) {
  FP32 *fvec = static_cast<FP32 *>(vec);
  if (m_isParallelBuild) {
    //m_batch.insert(m_batch.end(), fvec, fvec + m_dim);
    m_batch.insert(m_batch.end(), (char *)vec, ((char *)vec + m_space->get_data_size()));
    m_batchkeys.push_back(id);

    if (m_batchkeys.size() == HNSW_PARALLEL_BUILD_UNIT_SIZE) {
      flushBatchParallel();
    }
  } else {
    m_alg_hnsw->addPoint(fvec, id);
  }

  m_n_rows++; // atomic
  m_isDirty = true;
  return true;
}

class VectorIndexCollection
{
  public:
    AbstractVectorIndex* open(const string& name,
                              const string& options,
                              const string& useraction);

    AbstractVectorIndex* get(const string &name);

    bool                 close(AbstractVectorIndex *hindex);

  private:
    unordered_map<string, AbstractVectorIndex*> m_indexes;
    mutex m_mutex;
};

class SharedLockGuard {
  public:
    SharedLockGuard(AbstractVectorIndex *h_index) : m_index(h_index) {}
    ~SharedLockGuard() { if (m_index) m_index->unlockShared(); }
  private:
     AbstractVectorIndex *m_index{nullptr};
};

AbstractVectorIndex* VectorIndexCollection::open(const string &name,
                const string &options, const string &useraction) {

  lock_guard<mutex> l(m_mutex);

  debug_print("Opening new index %s %s %s", name.c_str(), options.c_str(), useraction.c_str());

  AbstractVectorIndex *hnewindex = nullptr;

  /* First case handles both HNSW and HNSW_BV */
  if (options.rfind("type=HNSW") != string::npos) {
    hnewindex = new HNSWMemoryIndex(name, options);
  }
  else if (options.rfind("type=KNN") != string::npos) {
    hnewindex = new KNNIndex(name, options);
  }
  else {
    my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
      "MyVector unknown index type for %s options = %s, using KNN",
      name.c_str(), options.c_str());
    hnewindex = new KNNIndex(name, options);
  }

  m_indexes[name] = hnewindex;

  return hnewindex;
}

AbstractVectorIndex* VectorIndexCollection::get(const string &name)
{
  lock_guard<mutex> l(m_mutex);
  AbstractVectorIndex *hindex = nullptr;
  if (m_indexes.find(name) != m_indexes.end()) {
    hindex = m_indexes[name];
    hindex->lockShared(); /* acquire lock before returning from here */
  }
  else {  
    my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
      "VectorIndexCollection::get() index not found %s", name.c_str());
  }

  return hindex;
}

bool VectorIndexCollection::close(AbstractVectorIndex *hindex)
{
  lock_guard<mutex> l(m_mutex);

  hindex->unlockShared(); /* taken when opening the index */

  hindex->lockExclusive(); /* wait for all readers to drain */
  hindex->closeIndex();
  m_indexes.erase(m_indexes.find(hindex->getName()));
  hindex->unlockExclusive();

  delete hindex; /* no other thread can hold shared lock */
  return true;
}

static VectorIndexCollection g_indexes;

/* The MYVECTOR* Annotations supported by this plugin */
const string MYVECTOR_COLUMN_A = "MYVECTOR(";
const string MYVECTOR_IS_ANN_A = "MYVECTOR_IS_ANN(";
const string MYVECTOR_SEARCH_A = "MYVECTOR_SEARCH";
const string MYVECTOR_DEFAULT_INDEX_TYPE = "type=KNN";

const string MYVECTOR_IS_ANN_USAGE = "MYVECTOR_IS_ANN('<vector col>','<id col>','<search_vec>'[,'<options>'])";
const string MYVECTOR_SEARCH_USAGE = "MYVECTOR_SEARCH(baseTable,idColumn,vectorColumn,queryTable[,options])";


/* MySQL Column COMMENT max. length is 1024.e.g comment with all fields set :
 * MYVECTOR Column |type=HNSW,dim=1536,size=1000000,M=64,ef=100,track=updatets,threads=8
 */
const size_t MYVECTOR_MAX_COLUMN_INFO_LEN = 128;

/* For v1, let us restrict to 4096. OpenAI has 3072 dimension embeddings
 * now in model :  text-embedding-3-large.
 */
const size_t MYVECTOR_MAX_VECTOR_DIM      = 4096;

/* rewriteMyVectorColumnDef() - rewrite the MYVECTOR(...) annotation in
 * CREATE TABLE & ALTER TABLE.
 */
bool rewriteMyVectorColumnDef(const string &query, string &newQuery) {
   // support multiple MYVECTOR(...) columns 
   size_t pos;
   bool   error = false;

   newQuery = query;
   while ((pos = newQuery.find(MYVECTOR_COLUMN_A)) != string::npos)
   {
     size_t spos = pos + MYVECTOR_COLUMN_A.length();
     size_t epos = newQuery.find_first_of(')', pos);

     if (epos == string::npos)
     {
       my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
                             "MYVECTOR column terminating ')' not found.");
       error = true;
       break;
     }

     string colinfo = newQuery.substr(spos, (epos - spos));
     if (colinfo.length() > MYVECTOR_MAX_COLUMN_INFO_LEN) {
       my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
               "MYVECTOR column info too long, length = %d.", colinfo.length());
       error = true;
       break;
     }

     MyVectorOptions vo(colinfo);
     
     if (!vo.isValid()) {
       my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
         "MYVECTOR column options parse error, options=%s.", colinfo.c_str());
       error = true;
       break;
     }

     string vtype = vo.getOption("type");
     if (vtype == "") {
       colinfo = MYVECTOR_DEFAULT_INDEX_TYPE + "," + colinfo;
       vtype   = "KNN"; // TODO
       vo.setOption("type",vtype);
     }

     if (vo.getOption("dim") == "") {
       my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
                             "MYVECTOR column dimension not defined.");
       error = true;
       break;
     }

     bool addTrackingColumn = false;
     string trackingColumn;
     if (vo.getOption("track").length()) {
       addTrackingColumn = true;
       trackingColumn    = vo.getOption("track");
     }
     
     int dim = atoi(vo.getOption("dim").c_str());

     if (dim <= 1 || dim > MYVECTOR_MAX_VECTOR_DIM) {
       my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
                             "MYVECTOR column dimension incorrect %d.", dim);
       error = true;
       break;
     }

     size_t varblength = 0;
     if (vtype != "HNSW_BV")
       varblength = MyVectorStorageLength(dim);
     else
       varblength = MyVectorBVStorageLength(dim); // binary vector

     string newColumn;
     newColumn = "VARBINARY(" + to_string(varblength) +
                 ") COMMENT 'MYVECTOR Column |" + colinfo + "'";

     if (addTrackingColumn) {
       newColumn = newColumn + ", " + trackingColumn + " TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
     }
     newQuery = newQuery.substr(0, pos) + newColumn + newQuery.substr(epos+1);
   } // while

   my_plugin_log_message(&gplugin, MY_INFORMATION_LEVEL, 
                         "MYVECTOR column rewrite \n%s.", newQuery.c_str());
   return error;
}

/* rewriteMyVectorIsANN() - rewrite the "WHERE MYVECTOR_IS_ANN(...)" annotation */
bool rewriteMyVectorIsANN(const string &query, string &newQuery) {
   size_t pos;
   bool   error = false;

   newQuery = query;
   while ((pos = newQuery.find(MYVECTOR_IS_ANN_A)) != string::npos)
   {
     size_t spos = pos + MYVECTOR_IS_ANN_A.length();
     size_t epos = spos+1;
     int ob = 1;
     /* We can have nested () in MYVECTOR_IS_ANN
      * e.g MYVECTOR_IS_ANN(a,b,myvector_construct(...))
      */
     while (epos < newQuery.length()) {
       if (newQuery[epos] == '(') ob++;
       if (newQuery[epos] == ')') {
         ob--;
         if (!ob) break; // done
       }
       epos++;
     }
     string strparams = newQuery.substr(spos, (epos - spos));
     
     vector<string> annparams;
     split(strparams, annparams);

#if 0
     if (annparams.size() < 3 || annparams.size() > 4) {
       my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
         "Incorrect MYVECTOR_IS_ANN options : %s\nExample usage : %s",
         strparams.c_str(), MYVECTOR_IS_ANN_USAGE.c_str());
       error = true;
       break;
     }
#endif
     string idcolexpr = annparams[1];
     idcolexpr = idcolexpr.substr(1, idcolexpr.length()-2); // remove the single quote
     
     stringstream ss;
     ss << "( " << idcolexpr << " IN "
        << "(select `myvecid` from JSON_TABLE(myvector_ann_set(" << strparams
        << "), " << '"' << "$[*]" << '"'
        << " COLUMNS(`myvecid` BIGINT PATH \"$\")) `myvector_ann`) )";
     
     newQuery = newQuery.substr(0, pos) +
                ss.str() +
                newQuery.substr(epos+1);
   } // while

   my_plugin_log_message(&gplugin, MY_INFORMATION_LEVEL, 
                         "MYVECTOR_IS_ANN query rewrite \n%s.",
                         newQuery.c_str());
   return error;
}

/* rewriteMyVectorSearch - rewrite the MYVECTOR_SEARCH[...] annotation */
bool rewriteMyVectorSearch(const string &query, string &newQuery)
{
   bool error = false;
   size_t pos;

   newQuery = query;
   string delim;
   /* No nested [] in MYVECTOR_SEARCH[...] */
   while ((pos = newQuery.find(MYVECTOR_SEARCH_A)) != string::npos) {
     size_t spos = pos + MYVECTOR_SEARCH_A.length();
     size_t epos;
     if (newQuery[spos] == '[')
       delim = "]";
     else if (newQuery[spos] == '{')
       delim = "}";
     else {
       error = true;
       break;
     }
     spos += delim.length();
     if ((epos = newQuery.find(delim, spos)) == string::npos) {
       error = true;
       break;
     }

     string strparams = newQuery.substr(spos, (epos - spos));
     
     vector<string> annparams;
     split(strparams, annparams);
     
     if (annparams.size() < 4 || annparams.size() > 5) {
       my_plugin_log_message(&gplugin, MY_ERROR_LEVEL, 
              "Incorrect MYVECTOR_SEARCH syntax : %s\nExample usage : %s",
              strparams.c_str(), MYVECTOR_SEARCH_USAGE.c_str());
       error = true;
       break;
     }

/*  select article5 from MYVECTOR_SEARCH[test.t1, id, test.t1.v1, query, n=5]; */

     string basetable = annparams[0];
     string idcol     = annparams[1];
     string vecindex  = annparams[2];
     string queryt    = annparams[3];

     string annopt    = "";
     if (annparams.size() > 4)
       annopt    = annparams[4];
   
     stringstream ss;
     ss << basetable << " where " << idcol
        << " in (select myvecid from " << queryt << " b, json_table(myvector_ann_set('"
        << vecindex << "','" << idcol << "', searchvec, '" << annopt << "') , " << '"'
        << "$[*]" << '"' << " COLUMNS(`myvecid` BIGINT PATH " << '"' << "$" << '"'
        << ")) `myvector_ann`)"; // 'searchvec' is hard-coded column name
     
     newQuery = newQuery.substr(0, pos) +
                ss.str() +
                newQuery.substr(epos+delim.length());
   }
   debug_print("MYVECTOR_SEARCH query rewrite=\n%s.", newQuery.c_str());

   return error;
}

/* myvector_query_rewriter() - Entrypoint of pre-parse query rewrite by this
 * plugin. This routine looks for CREATE TABLE, ALTER TABLE, SELECT and 
 * EXPLAIN and presence of MYVECTOR and proceeds to perform  the query
 * transformation.
 */
bool myvector_query_rewrite(const string &query, string *rewritten_query) {

  /* Check if SQL is CREATE/ALTER/SELECT/EXPLAIN */
  if (query.length() == 0 || !strchr("CcAaSsEe", query[0])) return false;

  /* quick top-level check and exit if no MYVECTOR* pattern found in query */
  if (!strstr(query.c_str(), "MYVECTOR")) return false;

  static const regex create_table("^CREATE\\s+TABLE",
                                regex::icase | regex::nosubs);
  static const regex altert_table("^ALTER\\s+TABLE",
                                regex::icase | regex::nosubs);
  static const regex select_stmt("^SELECT\\s+",
                                regex::icase | regex::nosubs);
  static const regex explain_stmt("^EXPLAIN\\s+",
                                regex::icase | regex::nosubs);
  

  string newQuery = "";
  *rewritten_query = query;

  if ((regex_search(query, select_stmt) || regex_search(query, explain_stmt))) {
    if (strstr(query.c_str(), MYVECTOR_IS_ANN_A.c_str())) {
      if (rewriteMyVectorIsANN(query, newQuery)) {
         newQuery = "";
       }
    }
    else if (strstr(query.c_str(), MYVECTOR_SEARCH_A.c_str())) {
      if (rewriteMyVectorSearch(query, newQuery)) {
         newQuery = "";
      }
    }
  }
  else if ((regex_search(query, create_table) || regex_search(query, altert_table))
        && (strstr(query.c_str(), MYVECTOR_COLUMN_A.c_str()))) {
      if (rewriteMyVectorColumnDef(query, newQuery)) {
        newQuery = "";
      }
  }

  if (newQuery.length())
    *rewritten_query = newQuery;

  return (*rewritten_query != query);
}

extern "C" bool myvector_ann_set_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  initid->ptr = nullptr;
  if (args->arg_count < 3 || args->arg_count > 4) {
    strcpy(message, "Incorrect arguments, usage : "
           "myvector_ann_set('vec column', 'id column', searchvec [,nn=<n>]).");
    return true; // error
  }

  /* Users can possibly ask for 100s of neighbours. With buffer of 128000,
   * about 12800 PK ids can be filled in the return string
   */
  initid->max_length = MYVECTOR_DISPLAY_MAX_LEN;
  initid->ptr        = (char *)malloc(initid->max_length);
  (*h_udf_metadata_service)->result_set(initid, "charset", latin1);
  return false;
}

extern "C" void myvector_ann_set_deinit(UDF_INIT *initid) {
  if (initid && initid->ptr) free(initid->ptr);
}

extern "C" char* myvector_ann_set(UDF_INIT *initid, UDF_ARGS *args, char *result,
                          unsigned long *length, unsigned char *is_null,
                          unsigned char *error) {
  char *col                 = args->args[0];
  char *idcol               = args->args[1];
  FP32 *searchvec           = (FP32 *)args->args[2];
  const char *searchoptions = nullptr;
  
  if (!col || !idcol || !searchvec) {
    *error = 1;
    *is_null = 1;
    return initid->ptr;
  }
  
  if (args->arg_count == 4) searchoptions = args->args[3];

  int nn = MYVECTOR_DEFAULT_ANN_RETURN_COUNT;
  if (searchoptions && args->lengths[3]) {
    MyVectorOptions vo(searchoptions);
    string          nstr = vo.getOption("nn"); /* How many neighbours to return? */

    if (nstr.length()) nn = atoi(nstr.c_str());
    if (nn <= 0)       nn = MYVECTOR_DEFAULT_ANN_RETURN_COUNT;
    
    nn = min((const unsigned int)nn, MYVECTOR_MAX_ANN_RETURN_COUNT);
  }
  
  AbstractVectorIndex *vi = g_indexes.get(col);
  SharedLockGuard l(vi);
  
  stringstream ss;
  if (vi && searchvec) {
    vector<KeyTypeInteger> result;
    vi->searchVectorNN(searchvec, vi->getDimension(), result, nn);

    /* simple JSON list of neighbour rows Pkid */
    ss <<  "[";
    for (int i = 0; i < result.size(); i++) {
      if (i) ss << ",";
      ss << result[i];
    } 
    ss << "]";
  }
  else {
    *is_null = 1;
    *error   = 1;
    ss << "[NULL]";
  }

  *length = ss.str().length();
  result = initid->ptr;
  strcpy(result, ss.str().c_str());
  return result;
}

/* SQFloatVectorToBinaryVector - Simple scalar quantization to convert
 * a sequence of natice floats to a binary vector. If the float value is
 * greater than 0, then corresponding bit is set to 1 in the binary vector.
 */
int SQFloatVectorToBinaryVector(FP32 *fvec, unsigned long *ivec, int dim)
{
  memset(ivec, 0, (dim / BITS_PER_BYTE ));  // 3rd param is bytes

  unsigned long elem = 0;
  unsigned long idx  = 0;

  for (int i = 0; i < dim; i++) {
    elem = elem << 1;
    if (fvec[i] > 0) {
      elem = elem | 1;
    }
    if (((i + 1) % 64) == 0) { // 8 bytes packed (i.e 64 dims in 1 ulong)
      ivec[idx] = elem;
      elem = 0; idx++;
    }
  }
  return (idx * sizeof(unsigned long)); // number of bytes
}

char *myvector_construct_bv(const std::string &srctype, char *src, char *dst,
                          unsigned long srclen, unsigned long *length,
                          unsigned char *is_null, unsigned char *error) {
  int retlen = 0;

  if (srctype == "bv") {
    memcpy(dst, src, srclen); // src is bytes representing the binary vector
    retlen = srclen;
  }
  else if (srctype == "float") {
    int dim = MyVectorDimFromStorageLength(srclen);
    retlen =
      SQFloatVectorToBinaryVector((FP32 *)src, (unsigned long *)dst, dim);
  }
  else if (srctype == "string") {
    char *start = nullptr, *ptr = nullptr;
    char endch;

    if ((start = strchr(src, '[')))
      endch = ']';
    else if ((start = strchr(src, '{')))
      endch = '}';
    else if ((start = strchr(src, '(')))
      endch = ')';
    else
    {
      start = src;
      endch = '\0';
    }
    if (endch) start++;

    ptr = start;

    while (*ptr && *ptr != endch) {
      while (*ptr && (*ptr == ' ' || *ptr == ',')) ptr++;
      char *p1 = ptr;
      while (*ptr != ' ' && *ptr != ',' && *ptr != endch) ptr++;
      char buff[64];
      
      strncpy(buff,p1,(ptr-p1));
      buff[(int)(ptr - p1)] = 0;

      dst[retlen] = (unsigned char)(atoi(buff)); 
    
      retlen += sizeof(unsigned char);
    } // while
  } // else


  unsigned int metadata = MYVECTOR_V1_BV_METADATA;
  memcpy(&dst[retlen], &metadata, sizeof(metadata));
  retlen += sizeof(metadata);
 
  ha_checksum cksum = my_checksum(0, (const unsigned char *)dst, retlen);
  memcpy(&dst[retlen], &cksum, sizeof(cksum));
  retlen += sizeof(cksum);

  *length = retlen;
  return dst;
}


extern "C" bool myvector_construct_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
        initid->max_length = MYVECTOR_CONSTRUCT_MAX_LEN;
        initid->ptr = (char *)malloc(MYVECTOR_CONSTRUCT_MAX_LEN);
        return false;

}

/* SQL function : MYVECTOR_CONSTRUCT(embedding_string)
 *   Input   : Embedding vector string e.g
 *             "[-0.006929283495992422 -0.005336422007530928 ...]"
 *   Returns : Serialized "myvector" to store in VARBINARY column.
 *             Each floating point value in the embedding vector string is
 *             converted to a 4-byte "FP32" (IEEE754). At the end, 2 4-byte
 *             integers are added :-
 *             1) 4-byte Metadata
 *             2) 4-byte Checksum (computed from all the FP32s plus Metadata)
 *
 *             If input string is NULL, then NULL is returned.
 */

extern "C" char *myvector_construct(UDF_INIT *initid, UDF_ARGS *args, char *result,
                          unsigned long *length, unsigned char *is_null,
                          unsigned char *error) {
  char *ptr = args->args[0];
  char *opt = args->args[1];
  char *start = nullptr;
  char endch;
  char *retvec = initid->ptr;
  int  retlen  = 0;
  bool skipConvert = false;

  if (!opt || !args->lengths[1])
    opt = "i=string,o=float"; // i=string,o=float
  else {
    MyVectorOptions vo(opt);

    /*
     * i = float, o = float : App already has the vector in floats, just
                              need to add MyVector metadata and checksum
     * i = bv,    o = bv    : App is sending bytes of a Binary Vector 
                              (e.g Cohere model). Add metadata + checksum
     * i = string, o = bv   : Convert series of 1-byte int's to  Binary
                              Vector
     * i = column, o = bv   : App wants to implement SQ compression. Convert
                              MyVector float column to BV
     */
    if (vo.getOption("i") == "float" && vo.getOption("o") == "float") 
      skipConvert = true;

    /* For Binary Vectors, we will branch out to a separate routine */
    if (vo.getOption("o") == "bv")
      return myvector_construct_bv(vo.getOption("i"), ptr, initid->ptr,
                                   args->lengths[0], length, is_null, error);
  } // else opt

  if (skipConvert) {
    // User is passing floats directly in bind variable or using "0x" literal
    if ((args->lengths[0] % sizeof(FP32)) != 0)
      SET_UDF_ERROR_AND_RETURN("Input vector is malformed, length not a multiple of sizeof(float) %lu.", args->lengths[0]);
    memcpy(retvec, ptr, args->lengths[0]);
    retlen = args->lengths[0];
    goto addChecksum;
  }

  /* Below code implements conversion from string "[0.134511 -0.082219 ...]" to
   * floats followed by metadata & checksum.
   */
  if ((start = strchr(ptr, '[')))
    endch = ']';
  else if ((start = strchr(ptr, '{')))
    endch = '}';
  else if ((start = strchr(ptr, '(')))
    endch = ')';
  else
  {
    start = ptr;
    endch = '\0';
  }
  if (endch) start++;

  ptr = start;

  while (*ptr && *ptr != endch) {
    while (*ptr && (*ptr == ' ' || *ptr == ',')) ptr++;
    char *p1 = ptr;
    while (*ptr != ' ' && *ptr != ',' && *ptr != endch) ptr++;
    char buff[64];
    strncpy(buff,p1,(ptr-p1));

    // TODO - atof() returns 0 if not a valid float
    FP32 fval = atof(buff); // change these 2 lines for FP16, INT8 etc
    memcpy(&retvec[retlen], &fval, sizeof(FP32)); 
    
    retlen += sizeof(FP32);
  } // while

addChecksum:
  unsigned int metadata = MYVECTOR_V1_FP32_METADATA;
  memcpy(&retvec[retlen], &metadata, sizeof(metadata));
  retlen += sizeof(metadata);
 
  ha_checksum cksum = my_checksum(0, (const unsigned char *)retvec, retlen);
  memcpy(&retvec[retlen], &cksum, sizeof(cksum));
  retlen += sizeof(cksum);

  *length = retlen;

  return retvec;
}


extern "C" void myvector_construct_deinit(UDF_INIT *initid) 
{ if (initid && initid->ptr) free(initid->ptr); }

extern "C" bool myvector_display_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count == 0 || args->arg_count > 2) {
    strcpy(message, "Incorrect arguments, usage : myvector_display(vec_col_expr, ['prec']).");
    return true; // error
  }
  initid->max_length = MYVECTOR_DISPLAY_MAX_LEN;
  initid->ptr = (char *)malloc(MYVECTOR_DISPLAY_MAX_LEN);
  (*h_udf_metadata_service)->result_set(initid, "charset", latin1);
  return false;
}

extern "C" char *myvector_display(UDF_INIT *initid, UDF_ARGS *args, char *result,
                          unsigned long *length, char *is_null,
                          char *error) {
  unsigned char *bvec = (unsigned char *)args->args[0];
  FP32 *fvec = (FP32 *)args->args[0];
  if (!bvec || !args->lengths[0]) {
    *is_null = 1;
    *error   = 1;
    return result;
  }

  /* We assume MySQL passes the VARBINARY value aligned correctly i.e aligned
   * at highest 8 bytes. Need to verify that when supporting double, FP16 etc.
   */

  int precision = 0;
  if (args->arg_count > 1 && args->args[1] && args->lengths[1]) {
    precision = atoi(args->args[1]);
  }
  if (!precision) precision = MYVECTOR_DISPLAY_DEF_PREC;
  
  stringstream ostr;

#if MYVECTOR_VERIFY_CHECKSUM
  ha_checksum cksum1;
  char        *raw = args->args[0];
  /* Checksum is at the end */
  memcpy((char *)&cksum1, &(raw[args->lengths[0] - sizeof(ha_checksum)]),
           sizeof(ha_checksum));
  ha_checksum cksum2 = my_checksum(0, (const unsigned char *)raw, 
                                   args->lengths[0] - sizeof(ha_checksum));
  if (cksum1 != cksum2) {
    *error   = 1;
    return 0.0;
  }
#endif

  int dim = 0;
  unsigned int metadata = 0;
  memcpy((char *)&metadata, &bvec[args->lengths[0] - MYVECTOR_COLUMN_EXTRA_LEN],
         sizeof(metadata));

  if (metadata == MYVECTOR_V1_FP32_METADATA) {
    bvec = nullptr;
    dim  = MyVectorDimFromStorageLength(args->lengths[0]);
  }
  else if (metadata == MYVECTOR_V1_BV_METADATA) {
    fvec = nullptr;
    dim  = MyVectorBVDimFromStorageLength(args->lengths[0]);
    dim  = dim / 8; /* bit-packet */
  }

  ostr << "[";
  ostr << setprecision(precision);
  for (int i = 0 ; i < dim; i++) {
    if (i) ostr << " ";
    if (fvec) {
      ostr << *fvec;
      fvec++;
    }
    else {
      ostr << (unsigned int)*bvec;
      bvec++;
    }
  }
  ostr << "]";

  result = initid->ptr;
  strcpy(result, ostr.str().c_str());
  *length = ostr.str().length();

  return result;
}

extern "C" void myvector_display_deinit(UDF_INIT *initid)
{ if (initid && initid->ptr) free(initid->ptr); }

/* UDF MYVECTOR_DISTANCE() Implementation */
extern "C" bool myvector_distance_init(UDF_INIT *, UDF_ARGS *args, char *message) {
  if (args->arg_count < 2) {
    strcpy(message, "myvector_distance() requires atleast 2 arguments.");
    return true; // error
  }
  if (args->arg_count > 3) {
    strcpy(message, "Too many arguments, usage : myvector_distance(v1,v2 [,dist]).");
    return true; // error
  }
  return false;
}

extern "C" bool myvector_construct_binaryvector_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 1) {
    strcpy(message, "Incorrect arguments, usage : myvector_construct_binary_vector(vec_col_expr)");
    return true; // error
  }
  initid->max_length = 1024;
  initid->ptr = (char *)malloc(initid->max_length);
  return false;
}


extern "C" bool myvector_hamming_distance_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  return false;
}

extern "C" double myvector_hamming_distance(UDF_INIT *, UDF_ARGS *args, char *is_null,
                          char *) {
  double dist = 0.0;
  unsigned long *v1 = (unsigned long *)(args->args[0]);
  unsigned long *v2 = (unsigned long *)(args->args[1]);

  size_t dim = args->lengths[0] * 8;

  return HammingDistanceFn(v1, v2, &dim);
}

extern "C" double myvector_distance(UDF_INIT *, UDF_ARGS *args, char *is_null,
                          char *error) {
  double dist = 0.0;
  FP32 *v1 = (FP32 *)(args->args[0]);
  FP32 *v2 = (FP32 *)(args->args[1]);
  int dim1 = MyVectorDimFromStorageLength(args->lengths[0]);
  int dim2 = MyVectorDimFromStorageLength(args->lengths[1]);

  /* Unsafe hack - keep going if 2 vectors have different dimension? */
  if (dim1 != dim2) {
    if (dim2 > dim1)
      dim2 = dim1;
    else
      dim1 = dim2;
  }

  if (!v1 || !v2  || (dim1 != dim2) || (dim1 <= 0)) {
    *error = 1;
    *is_null = 1;
    return 0.0;
  }

  const char *disttype = "L2"; // default
  if (args->arg_count == 3)
     disttype = args->args[2];

  double (*distfn)(FP32 *v1, FP32 *v2, int dim);
  // TODO : Cache function pointer in _init() if the 3rd argument is a constant literal
  if (!disttype) {
    *error = 1; // NULL distance measure
    return 0.0;
  }

  if (!strcasecmp(disttype, "L2") || !strcasecmp(disttype, "EUCLIDEAN"))
    distfn = computeL2Distance;
  else if (!strcasecmp(disttype, "Cosine"))
    distfn = computeCosineDistance;
  else if (!strcasecmp(disttype, "IP"))
    distfn = computeIPDistance;
  else {
    *error = 1; // Incorrect distance measure
    return 0.0;
  }

  dist = distfn(v1, v2, dim1);
  return dist;
}

extern "C" void myvector_distance_deinit(UDF_INIT *initid) {}


extern "C" bool myvector_search_open_udf_init(UDF_INIT *initid,
                UDF_ARGS *args, char *message) {
  if (args->arg_count != 5) {
    strcpy(message, "Incorrect arguments to MyVector internal UDF.");
    return true;
  }
  return false;
}

extern "C" char* myvector_search_open_udf(
                UDF_INIT *, UDF_ARGS *args, char *result,
                unsigned long *length, unsigned char *is_null,
                unsigned char *) {
    char *vecid   =  args->args[0];
    char *details =  args->args[1];
    char *pkidcol =  args->args[2];
    char *action  =  args->args[3];
    char *extra   =  args->args[4];
    bool existing =  true;
    char whereClause[1024] = "";

    my_plugin_log_message(&gplugin, MY_INFORMATION_LEVEL,
      "myvector_search_open() params %s %s %s %s %s",
      vecid, details, pkidcol, action, extra);

    strcpy(result, "SUCCESS");

    /* Admin operations usecases :-

     1. Load rows into base table -> call myvector("build"). If VectorIndex is already
        populated, it will be discarded and rebuilt. Persist index after build!
     2. Patch existing rows, delete a few rows, insert new rows -> call myvector("build")
     3. Any trouble with existing index  -> call myvector("drop")
     4. After reboot/restart VM -> call myvector("load") (or myvector("build"))
     5. If tracking column technique -> call myvector("refresh") - index should be loaded first.
        refresh will not persist. After reboot/restart, call myvector("load") followed by myvector("refresh")
     6. For explicit persist  -> call myvector("save"), needed after "refresh"
    */

    AbstractVectorIndex *vi = g_indexes.get(vecid);
    if (!vi) {
      vi = g_indexes.open(vecid, details, action);
      if (!vi) {
        strcpy(result, "Failed to open index");
        *length = strlen(result);
        return result;
      }
      existing = false;
    }

    string trackingColumn = "", threads = "";
    int    nthreads = 0;

    MyVectorOptions vo(details);
    if (vo.getOption("track").length()) {
       trackingColumn = vo.getOption("track");
    }
    if (vo.getOption("threads").length()) {
       threads  = vo.getOption("threads");
       nthreads = atoi(threads.c_str());
    }

    if (!strcmp(action, "save")) {
      vi->saveIndex(myvector_index_dir);
    }
    else if (!strcmp(action, "drop")) {
      vi->dropIndex(myvector_index_dir);
      g_indexes.close(vi);
      vi = nullptr;
    }
    else if (!strcmp(action, "load")) {
      vi->loadIndex(myvector_index_dir); // will handle 'reload' also
    }
    else if (!strcmp(action, "build")) {
      vi->dropIndex(myvector_index_dir);

      vi->initIndex(); // start new

      unsigned long currentts  = time(NULL);

      if (trackingColumn.length()) {
        snprintf(whereClause, sizeof(whereClause), " WHERE unix_timestamp(%s) <= %lu",
                 trackingColumn.c_str(), currentts);
      }
      vi->setUpdateTs(currentts);
      if (nthreads >=2 ) vi->startParallelBuild(nthreads);
    }
    else if (!strcmp(action, "refresh")) {
      unsigned long lastts = vi->getUpdateTs();
      if (!lastts)  {
      }

      unsigned long currentts  = time(NULL);
      if (trackingColumn.length()) {
        snprintf(whereClause, sizeof(whereClause),
          " WHERE unix_timestamp(%s) > %lu AND unix_timestamp(%s) <= %lu",
          trackingColumn.c_str(), lastts, trackingColumn.c_str(), currentts);
      }
      vi->setUpdateTs(currentts);
      if (nthreads >= 2) vi->startParallelBuild(nthreads);
    }

    if (strlen(whereClause))
      strcpy(result, whereClause);

    *length = strlen(result);
    return result;
}

extern "C" void myvector_search_open_udf_deinit() {}

extern "C" bool myvector_search_add_row_udf_init(UDF_INIT *initid,
                UDF_ARGS *args, char *message) {
  char *vecid   =  args->args[0];
  char *details =  args->args[1];

  /* Check if index is open and 'cache' it */
  if (!g_indexes.get(vecid)) {
    my_plugin_log_message(&gplugin, MY_ERROR_LEVEL,
              "Index %s is not opened for update.", vecid);
    return true;
  }

  initid->ptr = (char *)(g_indexes.get(vecid));
  return false;
}

/* UDF : myvector_search_add_row_udf() */
extern "C" long long myvector_search_add_row_udf(
                UDF_INIT *initid, UDF_ARGS *args, char *is_null,
                char *error)
{
  char *vecid   =  args->args[0];
  char *details =  args->args[1];

  long long pkid    = *((long long*) args->args[2]);
  char *vecval      = args->args[3];
  int  dims         = MyVectorDimFromStorageLength(args->lengths[3]);


  AbstractVectorIndex *vi = (AbstractVectorIndex *)(initid->ptr);
  if (vi) {
    vi->insertVector((FP32 *)vecval, dims, pkid);
  } else {
    *error = 1;
    return 0;
  }

  return 1; /* 1 row added to index */
}

/* UDF : myvector_search_add_row_udf() */
extern "C" void myvector_search_add_row_udf_deinit(UDF_INIT *initid) {
  /* build/rebuild/refresh complete - persist the index if it is capable */
  AbstractVectorIndex *vi = (AbstractVectorIndex *)(initid->ptr);

  if (vi) {
    my_plugin_log_message(&gplugin, MY_INFORMATION_LEVEL, 
      "Saving index %s to disk", vi->getName().c_str());

    vi->saveIndex(myvector_index_dir);

    my_plugin_log_message(&gplugin, MY_INFORMATION_LEVEL, 
      "Saving index %s to disk completed", vi->getName().c_str());

  }

  return;
}

extern "C" bool myvector_is_valid_init(UDF_INIT *initid, UDF_ARGS *args,
                                       char *message) {
  if (!initid || args->arg_count != 2) {
    strcpy(message, "Incorrect arguments to myvector_is_valid(), "
                    "Usage : myvector_is_valid(<vector>,<dim>)");
    return true;
  }
  return false;
}

/* myvector_is_valid : Verify vector checksum and dimension */
extern "C" long long myvector_is_valid(
                UDF_INIT *, UDF_ARGS *args, 
                unsigned char *, unsigned char *) {
  long long dim  = *((long long*) args->args[1]);

  /* A NULL vector OR a zero-dimension vector is not valid */
  if (!args->args[0] ||
      args->lengths[0] < (MYVECTOR_COLUMN_EXTRA_LEN + sizeof(FP32))) {
    return 0;
  }

  int vdim = MyVectorDimFromStorageLength(args->lengths[0]);
 
  if (vdim != dim) {
    return 0;
  }
  
  ha_checksum cksum1;
  char        *raw = args->args[0];

  /* Checksum is at the end */
  memcpy((char *)&cksum1, &(raw[args->lengths[0] - sizeof(ha_checksum)]),
           sizeof(ha_checksum));
  ha_checksum cksum2 = my_checksum(0, (const unsigned char *)raw, 
                                   args->lengths[0] - sizeof(ha_checksum));
  if (cksum1 != cksum2) {
    debug_print("myvector_is_valid checksum failure (%u != %u)",
                cksum1, cksum2);
    return 0;
  }
  
  return 1; // success
}

extern "C" void myvector_is_valid_deinit(UDF_INIT *) { }


/* end of myvector.cc */
